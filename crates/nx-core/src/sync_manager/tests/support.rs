use super::*;
use crate::sync_manager::schema::ensure_sync_schema;

pub(super) fn temp_store() -> Arc<NxStore> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    path.push(format!("numax-core-sync-test-{nanos}-{seq}"));
    let store = Arc::new(NxStore::open(path).unwrap());
    ensure_sync_schema(&store).unwrap();
    store
}

pub(super) fn metrics() -> Arc<RuntimeMetrics> {
    Arc::new(RuntimeMetrics::default())
}

pub(super) fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    addr.to_string()
}

pub(super) fn read_materialized(store: &NxStore, key: &str) -> u64 {
    let key = materialized_gcounter_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes);
    u64::from_le_bytes(buf)
}

pub(super) fn read_materialized_pncounter(store: &NxStore, key: &str) -> i64 {
    let key = materialized_pncounter_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes);
    i64::from_le_bytes(buf)
}

pub(super) fn read_durable_gcounter_state(store: &NxStore, key: &str) -> GCounter {
    let key = durable_gcounter_state_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    parse_durable_gcounter_state(&bytes).unwrap()
}

pub(super) fn read_durable_pncounter_state(store: &NxStore, key: &str) -> PNCounter {
    let key = durable_pncounter_state_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    parse_durable_pncounter_state(&bytes).unwrap()
}

pub(super) fn read_materialized_lww_register(store: &NxStore, key: &str) -> Vec<u8> {
    let key = materialized_lww_register_key(key);
    store.get(&key).unwrap().unwrap()
}

pub(super) fn read_durable_lww_register_state(store: &NxStore, key: &str) -> LwwRegister {
    let key = durable_lww_register_state_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    parse_durable_lww_register_state(&bytes).unwrap()
}

pub(super) fn read_materialized_lww_map(store: &NxStore, key: &str) -> Vec<(String, Vec<u8>)> {
    let key = materialized_lww_map_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

pub(super) fn read_durable_lww_map_state(store: &NxStore, key: &str) -> LwwMap {
    let key = durable_lww_map_state_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    parse_durable_lww_map_state(&bytes).unwrap()
}

pub(super) fn read_materialized_orset(store: &NxStore, key: &str) -> Vec<String> {
    let key = materialized_orset_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

pub(super) fn read_durable_orset_state(store: &NxStore, key: &str) -> ORSet {
    let key = durable_orset_state_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    parse_durable_orset_state(&bytes).unwrap()
}

pub(super) fn read_materialized_rga(store: &NxStore, key: &str) -> Vec<Vec<u8>> {
    let key = materialized_rga_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

pub(super) fn read_durable_rga_state(store: &NxStore, key: &str) -> Rga {
    let key = durable_rga_state_key(key);
    let bytes = store.get(&key).unwrap().unwrap();
    parse_durable_rga_state(&bytes).unwrap()
}

pub(super) fn seen_op_exists(store: &NxStore, op_id: &str) -> bool {
    store.get(&seen_op_store_key(op_id)).unwrap().is_some()
}

pub(super) fn write_durable_op_log_entry(store: &NxStore, key_op_id: &str, sequence: u64, op: &Op) {
    let key = op_log_store_key(key_op_id);
    let value = encode_durable_op_log_value(sequence, op).unwrap();
    store.set(&key, &value).unwrap();
}

pub(super) fn test_event_context(
    counters: Arc<RwLock<HashMap<String, GCounter>>>,
    seen_ops: Arc<RwLock<SeenOps>>,
    op_log: Arc<RwLock<Vec<Op>>>,
    store: Arc<NxStore>,
    metrics: Arc<RuntimeMetrics>,
    peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,
) -> NodeEventContext {
    NodeEventContext {
        counters,
        pncounters: Arc::new(RwLock::new(HashMap::new())),
        lww_registers: Arc::new(RwLock::new(HashMap::new())),
        lww_maps: Arc::new(RwLock::new(HashMap::new())),
        orsets: Arc::new(RwLock::new(HashMap::new())),
        rgas: Arc::new(RwLock::new(HashMap::new())),
        seen_ops,
        seen_ops_next_sequence: Arc::new(AtomicU64::new(0)),
        op_log,
        op_log_next_sequence: Arc::new(AtomicU64::new(0)),
        op_log_limit: 1024,
        store,
        metrics,
        node: Arc::new(Node::new(NodeConfig::new(
            NodeId::generate(),
            "127.0.0.1:0",
        ))),
        peer_health,
        peer_node_ids: Arc::new(RwLock::new(HashMap::new())),
        anti_entropy_watermarks: Arc::new(RwLock::new(HashMap::new())),
        peer_dead_after_failures: 2,
    }
}

pub(super) async fn apply_remote_op_for_test(
    op: &Op,
    counters: &Arc<RwLock<HashMap<String, GCounter>>>,
    seen_ops: &Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: &Arc<AtomicU64>,
    op_log: &Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: &Arc<AtomicU64>,
    store: &Arc<NxStore>,
) {
    let pncounters = Arc::new(RwLock::new(HashMap::new()));
    let lww_registers = Arc::new(RwLock::new(HashMap::new()));
    let lww_maps = Arc::new(RwLock::new(HashMap::new()));
    let orsets = Arc::new(RwLock::new(HashMap::new()));
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let metrics = metrics();
    let context = RemoteOpApplyContext {
        counters,
        pncounters: &pncounters,
        lww_registers: &lww_registers,
        lww_maps: &lww_maps,
        orsets: &orsets,
        rgas: &rgas,
        seen_ops,
        seen_ops_next_sequence,
        op_log,
        op_log_next_sequence,
        op_log_limit: 1024,
        store,
        metrics: &metrics,
    };
    apply_remote_ops(std::slice::from_ref(op), &context)
        .await
        .unwrap();
}

pub(super) async fn apply_remote_pncounter_op_for_test(
    op: &Op,
    pncounters: &Arc<RwLock<HashMap<String, PNCounter>>>,
    seen_ops: &Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: &Arc<AtomicU64>,
    op_log: &Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: &Arc<AtomicU64>,
    store: &Arc<NxStore>,
) {
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let lww_registers = Arc::new(RwLock::new(HashMap::new()));
    let lww_maps = Arc::new(RwLock::new(HashMap::new()));
    let orsets = Arc::new(RwLock::new(HashMap::new()));
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let metrics = metrics();
    let context = RemoteOpApplyContext {
        counters: &counters,
        pncounters,
        lww_registers: &lww_registers,
        lww_maps: &lww_maps,
        orsets: &orsets,
        rgas: &rgas,
        seen_ops,
        seen_ops_next_sequence,
        op_log,
        op_log_next_sequence,
        op_log_limit: 1024,
        store,
        metrics: &metrics,
    };
    apply_remote_ops(std::slice::from_ref(op), &context)
        .await
        .unwrap();
}

pub(super) async fn apply_remote_lww_register_op_for_test(
    op: &Op,
    registers: &Arc<RwLock<HashMap<String, LwwRegister>>>,
    seen_ops: &Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: &Arc<AtomicU64>,
    op_log: &Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: &Arc<AtomicU64>,
    store: &Arc<NxStore>,
) {
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let pncounters = Arc::new(RwLock::new(HashMap::new()));
    let lww_maps = Arc::new(RwLock::new(HashMap::new()));
    let orsets = Arc::new(RwLock::new(HashMap::new()));
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let metrics = metrics();
    let context = RemoteOpApplyContext {
        counters: &counters,
        pncounters: &pncounters,
        lww_registers: registers,
        lww_maps: &lww_maps,
        orsets: &orsets,
        rgas: &rgas,
        seen_ops,
        seen_ops_next_sequence,
        op_log,
        op_log_next_sequence,
        op_log_limit: 1024,
        store,
        metrics: &metrics,
    };
    apply_remote_ops(std::slice::from_ref(op), &context)
        .await
        .unwrap();
}

pub(super) async fn apply_remote_lww_map_op_for_test(
    op: &Op,
    maps: &Arc<RwLock<HashMap<String, LwwMap>>>,
    seen_ops: &Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: &Arc<AtomicU64>,
    op_log: &Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: &Arc<AtomicU64>,
    store: &Arc<NxStore>,
) {
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let pncounters = Arc::new(RwLock::new(HashMap::new()));
    let lww_registers = Arc::new(RwLock::new(HashMap::new()));
    let orsets = Arc::new(RwLock::new(HashMap::new()));
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let metrics = metrics();
    let context = RemoteOpApplyContext {
        counters: &counters,
        pncounters: &pncounters,
        lww_registers: &lww_registers,
        lww_maps: maps,
        orsets: &orsets,
        rgas: &rgas,
        seen_ops,
        seen_ops_next_sequence,
        op_log,
        op_log_next_sequence,
        op_log_limit: 1024,
        store,
        metrics: &metrics,
    };
    apply_remote_ops(std::slice::from_ref(op), &context)
        .await
        .unwrap();
}

pub(super) async fn apply_remote_orset_op_for_test(
    op: &Op,
    sets: &Arc<RwLock<HashMap<String, ORSet>>>,
    seen_ops: &Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: &Arc<AtomicU64>,
    op_log: &Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: &Arc<AtomicU64>,
    store: &Arc<NxStore>,
) {
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let pncounters = Arc::new(RwLock::new(HashMap::new()));
    let lww_registers = Arc::new(RwLock::new(HashMap::new()));
    let lww_maps = Arc::new(RwLock::new(HashMap::new()));
    let rgas = Arc::new(RwLock::new(HashMap::new()));
    let metrics = metrics();
    let context = RemoteOpApplyContext {
        counters: &counters,
        pncounters: &pncounters,
        lww_registers: &lww_registers,
        lww_maps: &lww_maps,
        orsets: sets,
        rgas: &rgas,
        seen_ops,
        seen_ops_next_sequence,
        op_log,
        op_log_next_sequence,
        op_log_limit: 1024,
        store,
        metrics: &metrics,
    };
    apply_remote_ops(std::slice::from_ref(op), &context)
        .await
        .unwrap();
}

pub(super) async fn apply_remote_rga_op_for_test(
    op: &Op,
    rgas: &Arc<RwLock<HashMap<String, Rga>>>,
    seen_ops: &Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: &Arc<AtomicU64>,
    op_log: &Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: &Arc<AtomicU64>,
    store: &Arc<NxStore>,
) {
    let counters = Arc::new(RwLock::new(HashMap::new()));
    let pncounters = Arc::new(RwLock::new(HashMap::new()));
    let lww_registers = Arc::new(RwLock::new(HashMap::new()));
    let lww_maps = Arc::new(RwLock::new(HashMap::new()));
    let orsets = Arc::new(RwLock::new(HashMap::new()));
    let metrics = metrics();
    let context = RemoteOpApplyContext {
        counters: &counters,
        pncounters: &pncounters,
        lww_registers: &lww_registers,
        lww_maps: &lww_maps,
        orsets: &orsets,
        rgas,
        seen_ops,
        seen_ops_next_sequence,
        op_log,
        op_log_next_sequence,
        op_log_limit: 1024,
        store,
        metrics: &metrics,
    };
    apply_remote_ops(std::slice::from_ref(op), &context)
        .await
        .unwrap();
}

pub(super) async fn wait_for_counter(manager: &SyncManager, key: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager.get_counter_value(key).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "counter {key} did not reach {expected}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn wait_for_pncounter(manager: &SyncManager, key: &str, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager.get_pncounter_value(key).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "pncounter {key} did not reach {expected}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn wait_for_lww_register(manager: &SyncManager, key: &str, expected: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager.get_lww_register_value(key).await.as_deref() == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "lww-register {key} did not reach expected value"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn wait_for_orset(manager: &SyncManager, key: &str, expected: &[&str]) {
    let expected = expected
        .iter()
        .map(|element| (*element).to_string())
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager.get_orset_elements(key).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "orset {key} did not reach expected elements {expected:?}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn wait_for_lww_map(manager: &SyncManager, key: &str, expected: &[(&str, &[u8])]) {
    let expected = expected
        .iter()
        .map(|(field, value)| ((*field).to_string(), (*value).to_vec()))
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager.get_lww_map_entries(key).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "lww-map {key} did not reach expected entries"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn wait_for_rga(manager: &SyncManager, key: &str, expected: &[&[u8]]) {
    let expected = expected
        .iter()
        .map(|value| (*value).to_vec())
        .collect::<Vec<_>>();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager.get_rga_values(key).await == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "rga {key} did not reach expected values"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn wait_for_connected_peer(manager: &SyncManager) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager.connected_peer_count().await > 0 {
            return;
        }
        assert!(Instant::now() < deadline, "manager did not connect to peer");
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn wait_for_peer_health(
    manager: &SyncManager,
    peer: &str,
    expected: PeerHealthState,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if manager.peer_health_state(peer).await == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "peer {peer} did not reach health state {expected:?}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn local_increment(handle: &SyncHandle, key: &str, delta: u64) {
    let op = apply_local_increment(handle, key, delta).await;
    handle.op_sender().send(op).await.unwrap();
}

pub(super) async fn local_pncounter_inc(handle: &SyncHandle, key: &str, delta: u64) {
    let op = apply_local_pncounter_inc(handle, key, delta).await;
    handle.op_sender().send(op).await.unwrap();
}

pub(super) async fn local_pncounter_dec(handle: &SyncHandle, key: &str, delta: u64) {
    let op = apply_local_pncounter_dec(handle, key, delta).await;
    handle.op_sender().send(op).await.unwrap();
}

pub(super) async fn local_lww_register_set(
    handle: &SyncHandle,
    key: &str,
    value: &[u8],
    timestamp_ms: u64,
) {
    let op = apply_local_lww_register_set(handle, key, value, timestamp_ms).await;
    handle.op_sender().send(op).await.unwrap();
}

pub(super) async fn local_orset_add(handle: &SyncHandle, key: &str, element: &str) {
    let op = apply_local_orset_add(handle, key, element).await;
    handle.op_sender().send(op).await.unwrap();
}

pub(super) async fn local_lww_map_set(
    handle: &SyncHandle,
    key: &str,
    field: &str,
    value: &[u8],
    timestamp_ms: u64,
) {
    let op = apply_local_lww_map_set(handle, key, field, value, timestamp_ms).await;
    handle.op_sender().send(op).await.unwrap();
}

pub(super) async fn local_lww_map_remove(
    handle: &SyncHandle,
    key: &str,
    field: &str,
    timestamp_ms: u64,
) {
    let op = apply_local_lww_map_remove(handle, key, field, timestamp_ms).await;
    handle.op_sender().send(op).await.unwrap();
}

pub(super) async fn local_orset_remove(handle: &SyncHandle, key: &str, element: &str) {
    if let Some(op) = apply_local_orset_remove(handle, key, element).await {
        handle.op_sender().send(op).await.unwrap();
    }
}

pub(super) async fn local_rga_insert_after(
    handle: &SyncHandle,
    key: &str,
    parent: Option<&str>,
    value: &[u8],
) -> String {
    let (id, op) = apply_local_rga_insert_after(handle, key, parent, value).await;
    handle.op_sender().send(op).await.unwrap();
    id
}

pub(super) async fn local_rga_delete(handle: &SyncHandle, key: &str, id: &str) {
    let op = apply_local_rga_delete(handle, key, id).await;
    handle.op_sender().send(op).await.unwrap();
}

pub(super) async fn dropped_local_increment(
    manager: &SyncManager,
    handle: &SyncHandle,
    key: &str,
    delta: u64,
) {
    let op = apply_local_increment(handle, key, delta).await;
    remember_ops(
        &manager.seen_ops,
        &manager.seen_ops_next_sequence,
        &manager.op_log,
        &manager.op_log_next_sequence,
        manager.config.op_log_limit,
        &manager.store,
        &[op],
    )
    .await
    .unwrap();
}

pub(super) async fn apply_local_increment(handle: &SyncHandle, key: &str, delta: u64) -> Op {
    {
        let counters_arc = handle.counters();
        let mut counters = counters_arc.write().await;
        let mut counter = counters.get(key).cloned().unwrap_or_else(GCounter::new);
        counter.increment(handle.node_id(), delta);

        persist_gcounter_state(&handle.store(), key, &counter).unwrap();
        counters.insert(key.to_string(), counter);
    }

    Op::gcounter_increment(handle.node_id().clone(), key, delta)
}

pub(super) async fn apply_local_pncounter_inc(handle: &SyncHandle, key: &str, delta: u64) -> Op {
    apply_local_pncounter_change(handle, key, delta, PNCounterChangeForTest::Increment).await
}

pub(super) async fn apply_local_pncounter_dec(handle: &SyncHandle, key: &str, delta: u64) -> Op {
    apply_local_pncounter_change(handle, key, delta, PNCounterChangeForTest::Decrement).await
}

#[derive(Debug, Clone, Copy)]
enum PNCounterChangeForTest {
    Increment,
    Decrement,
}

async fn apply_local_pncounter_change(
    handle: &SyncHandle,
    key: &str,
    delta: u64,
    change: PNCounterChangeForTest,
) -> Op {
    {
        let counters_arc = handle.pncounters();
        let mut counters = counters_arc.write().await;
        let mut counter = counters.get(key).cloned().unwrap_or_else(PNCounter::new);
        match change {
            PNCounterChangeForTest::Increment => counter.increment(handle.node_id(), delta),
            PNCounterChangeForTest::Decrement => counter.decrement(handle.node_id(), delta),
        }

        persist_pncounter_state(&handle.store(), key, &counter).unwrap();
        counters.insert(key.to_string(), counter);
    }

    match change {
        PNCounterChangeForTest::Increment => {
            Op::pncounter_increment(handle.node_id().clone(), key, delta)
        }
        PNCounterChangeForTest::Decrement => {
            Op::pncounter_decrement(handle.node_id().clone(), key, delta)
        }
    }
}

pub(super) async fn apply_local_lww_register_set(
    handle: &SyncHandle,
    key: &str,
    value: &[u8],
    timestamp_ms: u64,
) -> Op {
    {
        let registers_arc = handle.lww_registers();
        let mut registers = registers_arc.write().await;
        let candidate = LwwRegister::new(value.to_vec(), timestamp_ms, handle.node_id().clone());
        let register = registers
            .entry(key.to_string())
            .or_insert_with(|| candidate.clone());
        register.merge(&candidate);

        persist_lww_register_state(&handle.store(), key, register).unwrap();
    }

    Op::lww_register_set(handle.node_id().clone(), key, value.to_vec(), timestamp_ms)
}

pub(super) async fn apply_local_lww_map_set(
    handle: &SyncHandle,
    key: &str,
    field: &str,
    value: &[u8],
    timestamp_ms: u64,
) -> Op {
    {
        let maps_arc = handle.lww_maps();
        let mut maps = maps_arc.write().await;
        let mut map = maps.get(key).cloned().unwrap_or_else(LwwMap::new);
        map.set(
            field.to_string(),
            value.to_vec(),
            timestamp_ms,
            handle.node_id().clone(),
        );

        persist_lww_map_state(&handle.store(), key, &map).unwrap();
        maps.insert(key.to_string(), map);
    }

    Op::lww_map_set(
        handle.node_id().clone(),
        key,
        field,
        value.to_vec(),
        timestamp_ms,
    )
}

pub(super) async fn apply_local_lww_map_remove(
    handle: &SyncHandle,
    key: &str,
    field: &str,
    timestamp_ms: u64,
) -> Op {
    {
        let maps_arc = handle.lww_maps();
        let mut maps = maps_arc.write().await;
        let mut map = maps.get(key).cloned().unwrap_or_else(LwwMap::new);
        map.remove(field.to_string(), timestamp_ms, handle.node_id().clone());

        persist_lww_map_state(&handle.store(), key, &map).unwrap();
        maps.insert(key.to_string(), map);
    }

    Op::lww_map_remove(handle.node_id().clone(), key, field, timestamp_ms)
}

pub(super) async fn apply_local_orset_add(handle: &SyncHandle, key: &str, element: &str) -> Op {
    let op = Op::orset_add_with_op_id_tag(handle.node_id().clone(), key, element);
    let tag = match &op.kind {
        OpKind::ORSetAdd { tag, .. } => tag.clone(),
        other => panic!("unexpected op kind: {other:?}"),
    };

    {
        let sets_arc = handle.orsets();
        let mut sets = sets_arc.write().await;
        let mut set = sets.get(key).cloned().unwrap_or_else(ORSet::new);
        set.add(element, tag);

        persist_orset_state(&handle.store(), key, &set).unwrap();
        sets.insert(key.to_string(), set);
    }

    op
}

pub(super) async fn apply_local_orset_remove(
    handle: &SyncHandle,
    key: &str,
    element: &str,
) -> Option<Op> {
    let observed_tags = {
        let sets_arc = handle.orsets();
        let mut sets = sets_arc.write().await;
        let mut set = sets.get(key).cloned().unwrap_or_else(ORSet::new);
        let observed_tags = set.remove(element);
        if observed_tags.is_empty() {
            return None;
        }

        persist_orset_state(&handle.store(), key, &set).unwrap();
        sets.insert(key.to_string(), set);
        observed_tags
    };

    Some(Op::orset_remove(
        handle.node_id().clone(),
        key,
        element,
        observed_tags,
    ))
}

pub(super) async fn apply_local_rga_insert_after(
    handle: &SyncHandle,
    key: &str,
    parent: Option<&str>,
    value: &[u8],
) -> (String, Op) {
    let op = Op::rga_insert_with_op_id(
        handle.node_id().clone(),
        key,
        parent.map(ToOwned::to_owned),
        value.to_vec(),
    );
    let id = match &op.kind {
        OpKind::RgaInsert { id, .. } => id.clone(),
        other => panic!("unexpected op kind: {other:?}"),
    };

    {
        let rgas_arc = handle.rgas();
        let mut rgas = rgas_arc.write().await;
        let mut rga = rgas.get(key).cloned().unwrap_or_else(Rga::new);
        rga.insert(id.clone(), parent.map(ToOwned::to_owned), value.to_vec());

        persist_rga_state(&handle.store(), key, &rga).unwrap();
        rgas.insert(key.to_string(), rga);
    }

    (id, op)
}

pub(super) async fn apply_local_rga_delete(handle: &SyncHandle, key: &str, id: &str) -> Op {
    {
        let rgas_arc = handle.rgas();
        let mut rgas = rgas_arc.write().await;
        let mut rga = rgas.get(key).cloned().unwrap_or_else(Rga::new);
        rga.delete(id.to_string());

        persist_rga_state(&handle.store(), key, &rga).unwrap();
        rgas.insert(key.to_string(), rga);
    }

    Op::rga_delete(handle.node_id().clone(), key, id)
}

pub(super) async fn started_manager(addr: String) -> (SyncManager, SyncHandle, Arc<NxStore>) {
    let store = temp_store();
    let config = SyncConfig::new().with_listen_addr(addr);
    let mut manager = SyncManager::new(NodeId::generate(), config, Arc::clone(&store), metrics());
    let handle = manager.handle();
    manager.start().await.unwrap();
    (manager, handle, store)
}

pub(super) async fn started_manager_with_store(
    node_id: NodeId,
    config: SyncConfig,
    store: Arc<NxStore>,
) -> (SyncManager, SyncHandle) {
    let mut manager = SyncManager::new(node_id, config, store, metrics());
    let handle = manager.handle();
    manager.start().await.unwrap();
    (manager, handle)
}

pub(super) async fn started_manager_with_config(
    config: SyncConfig,
) -> (SyncManager, SyncHandle, Arc<NxStore>) {
    let store = temp_store();
    let mut manager = SyncManager::new(NodeId::generate(), config, Arc::clone(&store), metrics());
    let handle = manager.handle();
    manager.start().await.unwrap();
    (manager, handle, store)
}
