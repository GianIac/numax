use std::collections::HashMap;
use std::sync::atomic::Ordering;

use nx_sync::{GCounter, LwwMap, LwwRegister, ORSet, Op, OpKind, PNCounter, Rga};
use tracing::debug;

use super::replication::{
    apply_seen_insertions, plan_op_log_evictions, plan_seen_evictions,
    prune_op_log_and_return_evicted,
};
use super::storage::persist_remote_ops_batch;
use super::types::*;

pub(super) async fn apply_remote_ops(
    ops: &[Op],
    context: &RemoteOpApplyContext<'_>,
) -> anyhow::Result<()> {
    if ops.is_empty() {
        return Ok(());
    }

    let mut seen = context.seen_ops.write().await;
    let mut log = context.op_log.write().await;
    let mut counters = context.counters.write().await;
    let mut pncounters = context.pncounters.write().await;
    let mut lww_registers = context.lww_registers.write().await;
    let mut lww_maps = context.lww_maps.write().await;
    let mut orsets = context.orsets.write().await;
    let mut rgas = context.rgas.write().await;
    let mut next_seen_sequence = context.seen_ops_next_sequence.load(Ordering::Relaxed);
    let mut next_op_log_sequence = context.op_log_next_sequence.load(Ordering::Relaxed);
    let mut inserted_ids = Vec::new();
    let mut inserted_ops = Vec::new();
    let mut plans = Vec::new();
    let mut counter_updates = HashMap::new();
    let mut pncounter_updates = HashMap::new();
    let mut lww_register_updates = HashMap::new();
    let mut lww_map_updates = HashMap::new();
    let mut orset_updates = HashMap::new();
    let mut rga_updates = HashMap::new();

    for op in ops {
        let op_id = op.id.as_str();
        if seen.ids.contains(op_id) || inserted_ids.iter().any(|id| id == op_id) {
            debug!(op_id = %op.id, "skipping duplicate op");
            continue;
        }

        let registries = RemoteCrdtRegistries {
            counters: &counters,
            pncounters: &pncounters,
            lww_registers: &lww_registers,
            lww_maps: &lww_maps,
            orsets: &orsets,
            rgas: &rgas,
        };
        let mut updates = RemoteCrdtUpdates {
            counters: &mut counter_updates,
            pncounters: &mut pncounter_updates,
            lww_registers: &mut lww_register_updates,
            lww_maps: &mut lww_map_updates,
            orsets: &mut orset_updates,
            rgas: &mut rga_updates,
        };
        apply_remote_op_to_crdt_updates(op, &registries, &mut updates);

        inserted_ids.push(op_id.to_string());
        inserted_ops.push(op.clone());
    }

    let seen_evicted = plan_seen_evictions(&seen, &inserted_ids);
    let op_log_evicted = plan_op_log_evictions(&log, inserted_ops.len(), context.op_log_limit);
    let last_insert_index = inserted_ops.len().saturating_sub(1);

    for (index, op) in inserted_ops.iter().enumerate() {
        plans.push(OpPersistencePlan {
            op: op.clone(),
            seen_sequence: next_seen_sequence,
            op_log_sequence: next_op_log_sequence,
            seen_evicted: if index == last_insert_index {
                seen_evicted.clone()
            } else {
                Vec::new()
            },
            op_log_evicted: if index == last_insert_index {
                op_log_evicted.clone()
            } else {
                Vec::new()
            },
        });
        next_seen_sequence = next_seen_sequence.saturating_add(1);
        next_op_log_sequence = next_op_log_sequence.saturating_add(1);
    }

    persist_remote_ops_batch(
        context.store,
        &plans,
        RemoteCrdtUpdateBatch {
            counters: &counter_updates,
            pncounters: &pncounter_updates,
            lww_registers: &lww_register_updates,
            lww_maps: &lww_map_updates,
            orsets: &orset_updates,
            rgas: &rga_updates,
        },
    )?;

    let applied_count = plans.len() as u64;
    apply_seen_insertions(&mut seen, &inserted_ids, &seen_evicted);
    log.extend(inserted_ops);
    prune_op_log_and_return_evicted(&mut log, context.op_log_limit);
    for (key, counter) in counter_updates {
        counters.insert(key, counter);
    }
    for (key, counter) in pncounter_updates {
        pncounters.insert(key, counter);
    }
    for (key, register) in lww_register_updates {
        lww_registers.insert(key, register);
    }
    for (key, map) in lww_map_updates {
        lww_maps.insert(key, map);
    }
    for (key, set) in orset_updates {
        orsets.insert(key, set);
    }
    for (key, rga) in rga_updates {
        rgas.insert(key, rga);
    }
    context
        .seen_ops_next_sequence
        .store(next_seen_sequence, Ordering::Relaxed);
    context
        .op_log_next_sequence
        .store(next_op_log_sequence, Ordering::Relaxed);

    if applied_count > 0 {
        context.metrics.record_ops(applied_count);
        debug!(count = applied_count, "applied remote ops batch");
    }

    Ok(())
}

pub(super) fn apply_remote_op_to_crdt_updates(
    op: &Op,
    registries: &RemoteCrdtRegistries<'_>,
    updates: &mut RemoteCrdtUpdates<'_>,
) {
    match &op.kind {
        OpKind::GCounterIncrement { key, increment } => {
            apply_remote_gcounter_increment(
                op,
                key,
                *increment,
                registries.counters,
                updates.counters,
            );
        }
        OpKind::PNCounterIncrement { key, .. } | OpKind::PNCounterDecrement { key, .. } => {
            apply_remote_pncounter_op(op, key, registries.pncounters, updates.pncounters);
        }
        OpKind::LwwRegisterSet {
            key,
            value,
            timestamp_ms,
        } => {
            apply_remote_lww_register_set(
                op,
                key,
                value,
                *timestamp_ms,
                registries.lww_registers,
                updates.lww_registers,
            );
        }
        OpKind::LwwMapSet {
            key,
            field,
            value,
            timestamp_ms,
        } => {
            apply_remote_lww_map_set(
                op,
                key,
                field,
                value,
                *timestamp_ms,
                registries.lww_maps,
                updates.lww_maps,
            );
        }
        OpKind::LwwMapRemove {
            key,
            field,
            timestamp_ms,
        } => {
            apply_remote_lww_map_remove(
                op,
                key,
                field,
                *timestamp_ms,
                registries.lww_maps,
                updates.lww_maps,
            );
        }
        OpKind::ORSetAdd { key, element, tag } => {
            apply_remote_orset_add(key, element, tag, registries.orsets, updates.orsets);
        }
        OpKind::ORSetRemove {
            key,
            element,
            observed_tags,
        } => {
            apply_remote_orset_remove(
                key,
                element,
                observed_tags,
                registries.orsets,
                updates.orsets,
            );
        }
        OpKind::RgaInsert {
            key,
            id,
            parent,
            value,
        } => {
            apply_remote_rga_insert(
                key,
                id,
                parent.as_deref(),
                value,
                registries.rgas,
                updates.rgas,
            );
        }
        OpKind::RgaDelete { key, id } => {
            apply_remote_rga_delete(key, id, registries.rgas, updates.rgas);
        }
    }
}

pub(super) fn apply_remote_gcounter_increment(
    op: &Op,
    key: &str,
    increment: u64,
    counters: &HashMap<String, GCounter>,
    counter_updates: &mut HashMap<String, GCounter>,
) {
    let counter = counter_updates
        .entry(key.to_string())
        .or_insert_with(|| counters.get(key).cloned().unwrap_or_else(GCounter::new));
    counter.increment(&op.origin, increment);
}

pub(super) fn apply_remote_pncounter_op(
    op: &Op,
    key: &str,
    counters: &HashMap<String, PNCounter>,
    counter_updates: &mut HashMap<String, PNCounter>,
) {
    let counter = counter_updates
        .entry(key.to_string())
        .or_insert_with(|| counters.get(key).cloned().unwrap_or_else(PNCounter::new));
    let _ = counter.apply_op(op);
}

pub(super) fn apply_remote_lww_register_set(
    op: &Op,
    key: &str,
    value: &[u8],
    timestamp_ms: u64,
    registers: &HashMap<String, LwwRegister>,
    register_updates: &mut HashMap<String, LwwRegister>,
) {
    if let Some(register) = register_updates.get_mut(key) {
        register.assign(value.to_vec(), timestamp_ms, op.origin.clone());
        return;
    }

    let Some(existing) = registers.get(key) else {
        register_updates.insert(
            key.to_string(),
            LwwRegister::new(value.to_vec(), timestamp_ms, op.origin.clone()),
        );
        return;
    };

    let mut register = existing.clone();
    if register.assign(value.to_vec(), timestamp_ms, op.origin.clone()) {
        register_updates.insert(key.to_string(), register);
    }
}

pub(super) fn apply_remote_lww_map_set(
    op: &Op,
    key: &str,
    field: &str,
    value: &[u8],
    timestamp_ms: u64,
    maps: &HashMap<String, LwwMap>,
    map_updates: &mut HashMap<String, LwwMap>,
) {
    let map = map_updates
        .entry(key.to_string())
        .or_insert_with(|| maps.get(key).cloned().unwrap_or_else(LwwMap::new));
    map.set(
        field.to_string(),
        value.to_vec(),
        timestamp_ms,
        op.origin.clone(),
    );
}

pub(super) fn apply_remote_lww_map_remove(
    op: &Op,
    key: &str,
    field: &str,
    timestamp_ms: u64,
    maps: &HashMap<String, LwwMap>,
    map_updates: &mut HashMap<String, LwwMap>,
) {
    let map = map_updates
        .entry(key.to_string())
        .or_insert_with(|| maps.get(key).cloned().unwrap_or_else(LwwMap::new));
    map.remove(field.to_string(), timestamp_ms, op.origin.clone());
}

pub(super) fn apply_remote_orset_add(
    key: &str,
    element: &str,
    tag: &str,
    sets: &HashMap<String, ORSet>,
    set_updates: &mut HashMap<String, ORSet>,
) {
    let set = set_updates
        .entry(key.to_string())
        .or_insert_with(|| sets.get(key).cloned().unwrap_or_else(ORSet::new));
    set.apply_add(element, tag);
}

pub(super) fn apply_remote_orset_remove(
    key: &str,
    element: &str,
    observed_tags: &[String],
    sets: &HashMap<String, ORSet>,
    set_updates: &mut HashMap<String, ORSet>,
) {
    let set = set_updates
        .entry(key.to_string())
        .or_insert_with(|| sets.get(key).cloned().unwrap_or_else(ORSet::new));
    set.apply_remove(element, observed_tags.iter().cloned());
}

pub(super) fn apply_remote_rga_insert(
    key: &str,
    id: &str,
    parent: Option<&str>,
    value: &[u8],
    rgas: &HashMap<String, Rga>,
    rga_updates: &mut HashMap<String, Rga>,
) {
    let rga = rga_updates
        .entry(key.to_string())
        .or_insert_with(|| rgas.get(key).cloned().unwrap_or_else(Rga::new));
    rga.apply_insert(id.to_string(), parent.map(str::to_string), value.to_vec());
}

pub(super) fn apply_remote_rga_delete(
    key: &str,
    id: &str,
    rgas: &HashMap<String, Rga>,
    rga_updates: &mut HashMap<String, Rga>,
) {
    let rga = rga_updates
        .entry(key.to_string())
        .or_insert_with(|| rgas.get(key).cloned().unwrap_or_else(Rga::new));
    rga.apply_delete(id.to_string());
}

// End of main code. Test below:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::RuntimeMetrics;
    use crate::sync_manager::storage::{
        durable_gcounter_state_key, materialized_gcounter_key, parse_durable_gcounter_state,
    };
    use nx_store::Store as NxStore;
    use nx_sync::NodeId;
    use std::sync::{Arc, atomic::AtomicU64};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::RwLock;

    fn temp_store() -> Arc<NxStore> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        path.push(format!("numax-core-sync-apply-test-{nanos}-{seq}"));
        Arc::new(NxStore::open(path).unwrap())
    }

    fn metrics() -> Arc<RuntimeMetrics> {
        Arc::new(RuntimeMetrics::default())
    }

    async fn apply_remote_gcounter_op_for_test(
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

        apply_remote_gcounter_op_for_test(
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
        let state_key = durable_gcounter_state_key("counter:visits");
        let state_bytes = store.get(&state_key).unwrap().unwrap();
        let state = parse_durable_gcounter_state(&state_bytes).unwrap();
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

        apply_remote_gcounter_op_for_test(
            &op,
            &counters,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;
        apply_remote_gcounter_op_for_test(
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
}
