use std::collections::HashMap;

use nx_store::Store as NxStore;
use nx_sync::{GCounter, LwwMap, LwwRegister, NodeId, ORSet, Op, PNCounter, Rga};
use tracing::{debug, warn};

use super::replication::{
    normalize_op_log_limit, normalize_seen_ops_limit, prune_op_log_and_return_evicted,
};
use super::types::*;

pub(crate) fn materialized_gcounter_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::GCounter, CrdtStoreNamespace::Materialized, key)
}

pub(super) fn durable_gcounter_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::GCounter, CrdtStoreNamespace::State, key)
}

pub(crate) fn materialized_pncounter_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::PNCounter, CrdtStoreNamespace::Materialized, key)
}

pub(super) fn durable_pncounter_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::PNCounter, CrdtStoreNamespace::State, key)
}

pub(crate) fn materialized_lww_register_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::LwwRegister, CrdtStoreNamespace::Materialized, key)
}

pub(super) fn durable_lww_register_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::LwwRegister, CrdtStoreNamespace::State, key)
}

pub(crate) fn materialized_lww_map_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::LwwMap, CrdtStoreNamespace::Materialized, key)
}

pub(super) fn durable_lww_map_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::LwwMap, CrdtStoreNamespace::State, key)
}

pub(crate) fn materialized_orset_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::ORSet, CrdtStoreNamespace::Materialized, key)
}

pub(super) fn durable_orset_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::ORSet, CrdtStoreNamespace::State, key)
}

pub(crate) fn materialized_rga_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::Rga, CrdtStoreNamespace::Materialized, key)
}

pub(super) fn durable_rga_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::Rga, CrdtStoreNamespace::State, key)
}

pub(super) fn seen_op_store_key(op_id: &str) -> Vec<u8> {
    prefixed_store_key(SEEN_OP_STORE_PREFIX, op_id)
}

pub(super) fn op_log_store_key(op_id: &str) -> Vec<u8> {
    prefixed_store_key(OP_LOG_STORE_PREFIX, op_id)
}

pub(super) fn crdt_store_key(kind: CrdtKind, namespace: CrdtStoreNamespace, key: &str) -> Vec<u8> {
    prefixed_store_key(kind.prefix(namespace), key)
}

pub(super) fn prefixed_store_key(prefix: &str, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + key.len());
    out.extend_from_slice(prefix.as_bytes());
    out.extend_from_slice(key.as_bytes());
    out
}

pub(super) fn logical_gcounter_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(
        store_key,
        CrdtKind::GCounter,
        CrdtStoreNamespace::Materialized,
    )
}

pub(super) fn logical_gcounter_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::GCounter, CrdtStoreNamespace::State)
}

pub(super) fn logical_pncounter_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(
        store_key,
        CrdtKind::PNCounter,
        CrdtStoreNamespace::Materialized,
    )
}

pub(super) fn logical_pncounter_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::PNCounter, CrdtStoreNamespace::State)
}

pub(super) fn logical_lww_register_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::LwwRegister, CrdtStoreNamespace::State)
}

pub(super) fn logical_lww_map_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::LwwMap, CrdtStoreNamespace::State)
}

pub(super) fn logical_orset_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::ORSet, CrdtStoreNamespace::State)
}

pub(super) fn logical_rga_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::Rga, CrdtStoreNamespace::State)
}

pub(super) fn logical_seen_op_id(store_key: &[u8]) -> anyhow::Result<String> {
    logical_key_for_prefix(store_key, SEEN_OP_STORE_PREFIX, "seen OpId")
}

pub(super) fn logical_op_log_id(store_key: &[u8]) -> anyhow::Result<String> {
    logical_key_for_prefix(store_key, OP_LOG_STORE_PREFIX, "op log OpId")
}

pub(super) fn logical_crdt_key(
    store_key: &[u8],
    kind: CrdtKind,
    namespace: CrdtStoreNamespace,
) -> anyhow::Result<String> {
    let namespace_label = match namespace {
        CrdtStoreNamespace::Materialized => "materialized",
        CrdtStoreNamespace::State => "durable state",
    };
    logical_key_for_prefix(
        store_key,
        kind.prefix(namespace),
        &format!("{namespace_label} {}", kind.label()),
    )
}

pub(super) fn logical_key_for_prefix(
    store_key: &[u8],
    prefix: &str,
    kind: &str,
) -> anyhow::Result<String> {
    let key = store_key
        .strip_prefix(prefix.as_bytes())
        .ok_or_else(|| anyhow::anyhow!("invalid {kind} key"))?;

    String::from_utf8(key.to_vec()).map_err(|e| anyhow::anyhow!("invalid UTF-8 in {kind} key: {e}"))
}

pub(super) fn parse_seen_op_sequence(bytes: &[u8]) -> anyhow::Result<u64> {
    let buf: [u8; 8] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid seen OpId sequence length: expected 8, got {}",
            bytes.len()
        )
    })?;

    Ok(u64::from_be_bytes(buf))
}

pub(super) fn parse_materialized_gcounter_value(bytes: &[u8]) -> anyhow::Result<u64> {
    let buf: [u8; 8] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid materialized GCounter value length: expected 8, got {}",
            bytes.len()
        )
    })?;

    Ok(u64::from_le_bytes(buf))
}

pub(super) fn parse_materialized_pncounter_value(bytes: &[u8]) -> anyhow::Result<i64> {
    let buf: [u8; 8] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid materialized PNCounter value length: expected 8, got {}",
            bytes.len()
        )
    })?;

    Ok(i64::from_le_bytes(buf))
}

pub(super) fn encode_durable_op_log_value(sequence: u64, op: &Op) -> anyhow::Result<Vec<u8>> {
    let op_bytes = op.to_bytes()?;
    let mut out = Vec::with_capacity(8 + op_bytes.len());
    out.extend_from_slice(&sequence.to_be_bytes());
    out.extend_from_slice(&op_bytes);
    Ok(out)
}

pub(super) fn parse_durable_op_log_value(bytes: &[u8]) -> anyhow::Result<(u64, Op)> {
    if bytes.len() < 8 {
        anyhow::bail!(
            "invalid op log value length: expected at least 8, got {}",
            bytes.len()
        );
    }
    let mut seq = [0u8; 8];
    seq.copy_from_slice(&bytes[..8]);
    let op = Op::from_bytes(&bytes[8..])
        .map_err(|e| anyhow::anyhow!("invalid durable op log JSON: {e}"))?;
    Ok((u64::from_be_bytes(seq), op))
}

pub(super) fn hydrate_gcounter_registry(
    store: &NxStore,
    node_id: &NodeId,
    counters: &mut HashMap<String, GCounter>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_gcounter_state(store, counters)?;
    let materialized_count = hydrate_materialized_gcounter_values(store, node_id, counters)?;

    debug!(
        durable_count,
        materialized_count,
        total = counters.len(),
        "hydrated GCounter registry from sled"
    );
    Ok(counters.len())
}

pub(super) fn hydrate_pncounter_registry(
    store: &NxStore,
    node_id: &NodeId,
    counters: &mut HashMap<String, PNCounter>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_pncounter_state(store, counters)?;
    let materialized_count = hydrate_materialized_pncounter_values(store, node_id, counters)?;

    debug!(
        durable_count,
        materialized_count,
        total = counters.len(),
        "hydrated PNCounter registry from sled"
    );
    Ok(counters.len())
}

pub(super) fn hydrate_lww_register_registry(
    store: &NxStore,
    registers: &mut HashMap<String, LwwRegister>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_lww_register_state(store, registers)?;

    debug!(
        durable_count,
        total = registers.len(),
        "hydrated LWW-Register registry from sled"
    );
    Ok(registers.len())
}

pub(super) fn hydrate_lww_map_registry(
    store: &NxStore,
    maps: &mut HashMap<String, LwwMap>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_lww_map_state(store, maps)?;

    debug!(
        durable_count,
        total = maps.len(),
        "hydrated LWW-Map registry from sled"
    );
    Ok(maps.len())
}

pub(super) fn hydrate_orset_registry(
    store: &NxStore,
    sets: &mut HashMap<String, ORSet>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_orset_state(store, sets)?;

    debug!(
        durable_count,
        total = sets.len(),
        "hydrated ORSet registry from sled"
    );
    Ok(sets.len())
}

pub(super) fn hydrate_rga_registry(
    store: &NxStore,
    rgas: &mut HashMap<String, Rga>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_rga_state(store, rgas)?;

    debug!(
        durable_count,
        total = rgas.len(),
        "hydrated RGA registry from sled"
    );
    Ok(rgas.len())
}

pub(super) fn hydrate_seen_ops(store: &NxStore, limit: usize) -> anyhow::Result<(SeenOps, u64)> {
    let limit = normalize_seen_ops_limit(limit);
    let entries = store.scan_prefix(SEEN_OP_STORE_PREFIX.as_bytes())?;
    let mut ordered = Vec::with_capacity(entries.len());
    let mut next_sequence = 0;

    for (store_key, value_bytes) in entries {
        let op_id = match logical_seen_op_id(&store_key) {
            Ok(op_id) => op_id,
            Err(e) => {
                warn!(error = %e, "skipping invalid seen OpId key");
                continue;
            }
        };
        let sequence = match parse_seen_op_sequence(&value_bytes) {
            Ok(sequence) => sequence,
            Err(e) => {
                warn!(op_id = %op_id, error = %e, "skipping invalid seen OpId metadata");
                continue;
            }
        };

        next_sequence = next_sequence.max(sequence.saturating_add(1));
        ordered.push((sequence, op_id));
    }

    ordered.sort_by_key(|(sequence, _)| *sequence);
    let mut seen = SeenOps::new(limit);
    let mut evicted = Vec::new();
    for (_, op_id) in ordered {
        if let SeenOpsInsert::Inserted {
            evicted: evicted_ids,
        } = seen.insert(op_id)
        {
            evicted.extend(evicted_ids);
        }
    }
    persist_seen_op_evictions(store, &evicted)?;

    debug!(
        count = seen.len(),
        next_sequence, "hydrated seen OpId metadata from sled"
    );
    Ok((seen, next_sequence))
}

pub(super) fn hydrate_op_log(store: &NxStore, limit: usize) -> anyhow::Result<(Vec<Op>, u64)> {
    let limit = normalize_op_log_limit(limit);
    let entries = store.scan_prefix(OP_LOG_STORE_PREFIX.as_bytes())?;
    let mut ordered = Vec::with_capacity(entries.len());
    let mut next_sequence = 0;

    for (store_key, value_bytes) in entries {
        let op_id = match logical_op_log_id(&store_key) {
            Ok(op_id) => op_id,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable op log key");
                continue;
            }
        };
        let (sequence, op) = match parse_durable_op_log_value(&value_bytes) {
            Ok(value) => value,
            Err(e) => {
                warn!(op_id = %op_id, error = %e, "skipping invalid durable op log entry");
                continue;
            }
        };
        if op.id.as_str() != op_id {
            warn!(
                key_op_id = %op_id,
                value_op_id = %op.id,
                "skipping durable op log entry with mismatched OpId"
            );
            continue;
        }

        next_sequence = next_sequence.max(sequence.saturating_add(1));
        ordered.push((sequence, op));
    }

    ordered.sort_by_key(|(sequence, _)| *sequence);
    let mut op_log = ordered.into_iter().map(|(_, op)| op).collect::<Vec<_>>();
    let evicted = prune_op_log_and_return_evicted(&mut op_log, limit);
    persist_op_log_evictions(store, &evicted)?;

    debug!(
        count = op_log.len(),
        next_sequence, "hydrated durable op log from sled"
    );
    Ok((op_log, next_sequence))
}

pub(super) fn hydrate_durable_gcounter_state(
    store: &NxStore,
    counters: &mut HashMap<String, GCounter>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(GCOUNTER_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_gcounter_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable GCounter state key");
                continue;
            }
        };
        let counter = match parse_durable_gcounter_state(&value_bytes) {
            Ok(counter) => counter,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable GCounter state");
                continue;
            }
        };

        materialize_gcounter_value(store, &key, counter.value())?;
        counters.insert(key, counter);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(super) fn hydrate_durable_pncounter_state(
    store: &NxStore,
    counters: &mut HashMap<String, PNCounter>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(PNCOUNTER_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_pncounter_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable PNCounter state key");
                continue;
            }
        };
        let counter = match parse_durable_pncounter_state(&value_bytes) {
            Ok(counter) => counter,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable PNCounter state");
                continue;
            }
        };

        materialize_pncounter_value(store, &key, counter.value())?;
        counters.insert(key, counter);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(super) fn hydrate_durable_lww_register_state(
    store: &NxStore,
    registers: &mut HashMap<String, LwwRegister>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(LWW_REGISTER_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_lww_register_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable LWW-Register state key");
                continue;
            }
        };
        let register = match parse_durable_lww_register_state(&value_bytes) {
            Ok(register) => register,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable LWW-Register state");
                continue;
            }
        };

        materialize_lww_register_value(store, &key, register.value())?;
        registers.insert(key, register);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(super) fn hydrate_durable_lww_map_state(
    store: &NxStore,
    maps: &mut HashMap<String, LwwMap>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(LWW_MAP_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_lww_map_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable LWW-Map state key");
                continue;
            }
        };
        let map = match parse_durable_lww_map_state(&value_bytes) {
            Ok(map) => map,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable LWW-Map state");
                continue;
            }
        };

        materialize_lww_map_entries(store, &key, &map.entries())?;
        maps.insert(key, map);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(super) fn hydrate_durable_orset_state(
    store: &NxStore,
    sets: &mut HashMap<String, ORSet>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(ORSET_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_orset_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable ORSet state key");
                continue;
            }
        };
        let set = match parse_durable_orset_state(&value_bytes) {
            Ok(set) => set,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable ORSet state");
                continue;
            }
        };

        materialize_orset_elements(store, &key, &set.elements())?;
        sets.insert(key, set);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(super) fn hydrate_durable_rga_state(
    store: &NxStore,
    rgas: &mut HashMap<String, Rga>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(RGA_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_rga_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable RGA state key");
                continue;
            }
        };
        let rga = match parse_durable_rga_state(&value_bytes) {
            Ok(rga) => rga,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable RGA state");
                continue;
            }
        };

        materialize_rga_values(store, &key, &rga.values())?;
        rgas.insert(key, rga);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(super) fn hydrate_materialized_gcounter_values(
    store: &NxStore,
    node_id: &NodeId,
    counters: &mut HashMap<String, GCounter>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(GCOUNTER_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_gcounter_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid materialized GCounter key");
                continue;
            }
        };
        let value = match parse_materialized_gcounter_value(&value_bytes) {
            Ok(value) => value,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid materialized GCounter value");
                continue;
            }
        };
        if counters.contains_key(&key) {
            continue;
        }

        let mut counter = GCounter::new();
        counter.increment(node_id, value);
        counters.insert(key, counter);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(super) fn hydrate_materialized_pncounter_values(
    store: &NxStore,
    node_id: &NodeId,
    counters: &mut HashMap<String, PNCounter>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(PNCOUNTER_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_pncounter_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid materialized PNCounter key");
                continue;
            }
        };
        let value = match parse_materialized_pncounter_value(&value_bytes) {
            Ok(value) => value,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid materialized PNCounter value");
                continue;
            }
        };
        if counters.contains_key(&key) {
            continue;
        }

        let mut counter = PNCounter::new();
        if value >= 0 {
            counter.increment(node_id, value as u64);
        } else {
            counter.decrement(node_id, value.unsigned_abs());
        }
        counters.insert(key, counter);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(crate) fn materialize_gcounter_value(
    store: &NxStore,
    key: &str,
    value: u64,
) -> anyhow::Result<()> {
    let store_key = materialized_gcounter_key(key);
    store.set(&store_key, &value.to_le_bytes())?;
    Ok(())
}

pub(crate) fn materialize_pncounter_value(
    store: &NxStore,
    key: &str,
    value: i64,
) -> anyhow::Result<()> {
    let store_key = materialized_pncounter_key(key);
    store.set(&store_key, &value.to_le_bytes())?;
    Ok(())
}

pub(crate) fn materialize_lww_register_value(
    store: &NxStore,
    key: &str,
    value: &[u8],
) -> anyhow::Result<()> {
    let store_key = materialized_lww_register_key(key);
    store.set(&store_key, value)?;
    Ok(())
}

pub(crate) fn materialize_lww_map_entries(
    store: &NxStore,
    key: &str,
    entries: &[(String, Vec<u8>)],
) -> anyhow::Result<()> {
    let store_key = materialized_lww_map_key(key);
    let entries_json = serde_json::to_vec(entries)?;
    store.set(&store_key, &entries_json)?;
    Ok(())
}

pub(crate) fn materialize_orset_elements(
    store: &NxStore,
    key: &str,
    elements: &[String],
) -> anyhow::Result<()> {
    let store_key = materialized_orset_key(key);
    let elements_json = serde_json::to_vec(elements)?;
    store.set(&store_key, &elements_json)?;
    Ok(())
}

pub(crate) fn materialize_rga_values(
    store: &NxStore,
    key: &str,
    values: &[Vec<u8>],
) -> anyhow::Result<()> {
    let store_key = materialized_rga_key(key);
    let values_json = serde_json::to_vec(values)?;
    store.set(&store_key, &values_json)?;
    Ok(())
}

pub(super) fn parse_durable_gcounter_state(bytes: &[u8]) -> anyhow::Result<GCounter> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable GCounter state: {e}"))?;
    GCounter::from_json(json).map_err(|e| anyhow::anyhow!("invalid durable GCounter JSON: {e}"))
}

pub(super) fn parse_durable_pncounter_state(bytes: &[u8]) -> anyhow::Result<PNCounter> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable PNCounter state: {e}"))?;
    PNCounter::from_json(json).map_err(|e| anyhow::anyhow!("invalid durable PNCounter JSON: {e}"))
}

pub(super) fn parse_durable_lww_register_state(bytes: &[u8]) -> anyhow::Result<LwwRegister> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable LWW-Register state: {e}"))?;
    LwwRegister::from_json(json)
        .map_err(|e| anyhow::anyhow!("invalid durable LWW-Register JSON: {e}"))
}

pub(super) fn parse_durable_lww_map_state(bytes: &[u8]) -> anyhow::Result<LwwMap> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable LWW-Map state: {e}"))?;
    LwwMap::from_json(json).map_err(|e| anyhow::anyhow!("invalid durable LWW-Map JSON: {e}"))
}

pub(super) fn parse_durable_orset_state(bytes: &[u8]) -> anyhow::Result<ORSet> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable ORSet state: {e}"))?;
    ORSet::from_json(json).map_err(|e| anyhow::anyhow!("invalid durable ORSet JSON: {e}"))
}

pub(super) fn parse_durable_rga_state(bytes: &[u8]) -> anyhow::Result<Rga> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable RGA state: {e}"))?;
    Rga::from_json(json).map_err(|e| anyhow::anyhow!("invalid durable RGA JSON: {e}"))
}

pub(crate) fn persist_gcounter_state(
    store: &NxStore,
    key: &str,
    counter: &GCounter,
) -> anyhow::Result<()> {
    let store_key = durable_gcounter_state_key(key);
    let state_json = counter.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_gcounter_value(store, key, counter.value())?;
    Ok(())
}

pub(crate) fn persist_pncounter_state(
    store: &NxStore,
    key: &str,
    counter: &PNCounter,
) -> anyhow::Result<()> {
    let store_key = durable_pncounter_state_key(key);
    let state_json = counter.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_pncounter_value(store, key, counter.value())?;
    Ok(())
}

pub(crate) fn persist_lww_register_state(
    store: &NxStore,
    key: &str,
    register: &LwwRegister,
) -> anyhow::Result<()> {
    let store_key = durable_lww_register_state_key(key);
    let state_json = register.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_lww_register_value(store, key, register.value())?;
    Ok(())
}

pub(crate) fn persist_lww_map_state(
    store: &NxStore,
    key: &str,
    map: &LwwMap,
) -> anyhow::Result<()> {
    let store_key = durable_lww_map_state_key(key);
    let state_json = map.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_lww_map_entries(store, key, &map.entries())?;
    Ok(())
}

pub(crate) fn persist_orset_state(store: &NxStore, key: &str, set: &ORSet) -> anyhow::Result<()> {
    let store_key = durable_orset_state_key(key);
    let state_json = set.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_orset_elements(store, key, &set.elements())?;
    Ok(())
}

pub(crate) fn persist_rga_state(store: &NxStore, key: &str, rga: &Rga) -> anyhow::Result<()> {
    let store_key = durable_rga_state_key(key);
    let state_json = rga.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_rga_values(store, key, &rga.values())?;
    Ok(())
}

#[cfg(test)]
pub(super) fn persist_seen_op_batch(
    store: &NxStore,
    op_id: &str,
    sequence: u64,
    evicted: &[String],
) -> anyhow::Result<()> {
    let seen_key = seen_op_store_key(op_id);
    let seen_value = sequence.to_be_bytes();
    let delete_keys = evicted
        .iter()
        .map(|op_id| seen_op_store_key(op_id))
        .collect::<Vec<_>>();
    let delete_refs = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();

    store.apply_batch(
        &[(seen_key.as_slice(), seen_value.as_slice())],
        &delete_refs,
    )?;
    Ok(())
}

pub(super) fn collect_seen_delete_keys(evicted: &[String]) -> Vec<Vec<u8>> {
    evicted
        .iter()
        .map(|op_id| seen_op_store_key(op_id))
        .collect()
}

pub(super) fn collect_op_log_delete_keys(evicted: &[Op]) -> Vec<Vec<u8>> {
    evicted
        .iter()
        .map(|op| op_log_store_key(op.id.as_str()))
        .collect()
}

pub(super) fn persist_seen_op_evictions(store: &NxStore, evicted: &[String]) -> anyhow::Result<()> {
    if evicted.is_empty() {
        return Ok(());
    }

    let delete_keys = evicted
        .iter()
        .map(|op_id| seen_op_store_key(op_id))
        .collect::<Vec<_>>();
    let delete_refs = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();
    store.apply_batch(&[], &delete_refs)?;
    Ok(())
}

pub(super) fn persist_op_log_evictions(store: &NxStore, evicted: &[Op]) -> anyhow::Result<()> {
    if evicted.is_empty() {
        return Ok(());
    }

    let delete_keys = collect_op_log_delete_keys(evicted);
    let delete_refs = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();
    store.apply_batch(&[], &delete_refs)?;
    Ok(())
}

pub(super) fn persist_local_ops_batch(
    store: &NxStore,
    plans: &[OpPersistencePlan],
) -> anyhow::Result<()> {
    if plans.is_empty() {
        return Ok(());
    }

    let mut set_keys = Vec::new();
    let mut set_values = Vec::new();
    let mut delete_keys = Vec::new();

    for plan in plans {
        set_keys.push(seen_op_store_key(plan.op.id.as_str()));
        set_values.push(plan.seen_sequence.to_be_bytes().to_vec());
        set_keys.push(op_log_store_key(plan.op.id.as_str()));
        set_values.push(encode_durable_op_log_value(plan.op_log_sequence, &plan.op)?);
        delete_keys.extend(collect_seen_delete_keys(&plan.seen_evicted));
        delete_keys.extend(collect_op_log_delete_keys(&plan.op_log_evicted));
    }

    let sets = set_keys
        .iter()
        .zip(set_values.iter())
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    let deletes = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();

    store.apply_batch(&sets, &deletes)?;
    Ok(())
}

// End of main code. Test below:
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_store() -> Arc<NxStore> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        path.push(format!("numax-core-sync-storage-test-{nanos}-{seq}"));
        Arc::new(NxStore::open(path).unwrap())
    }

    fn seen_op_exists(store: &NxStore, op_id: &str) -> bool {
        store.get(&seen_op_store_key(op_id)).unwrap().is_some()
    }

    fn write_durable_op_log_entry(store: &NxStore, key_op_id: &str, sequence: u64, op: &Op) {
        let key = op_log_store_key(key_op_id);
        let value = encode_durable_op_log_value(sequence, op).unwrap();
        store.set(&key, &value).unwrap();
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
}

pub(super) fn persist_remote_ops_batch(
    store: &NxStore,
    plans: &[OpPersistencePlan],
    updates: RemoteCrdtUpdateBatch<'_>,
) -> anyhow::Result<()> {
    if plans.is_empty() {
        return Ok(());
    }

    let mut set_keys = Vec::new();
    let mut set_values = Vec::new();
    let mut delete_keys = Vec::new();
    let mut changed_counter_keys = updates.counters.keys().collect::<Vec<_>>();
    changed_counter_keys.sort();

    for key in changed_counter_keys {
        let Some(counter) = updates.counters.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_gcounter_state_key(key));
        set_values.push(counter.to_json()?.into_bytes());
        set_keys.push(materialized_gcounter_key(key));
        set_values.push(counter.value().to_le_bytes().to_vec());
    }

    let mut changed_pncounter_keys = updates.pncounters.keys().collect::<Vec<_>>();
    changed_pncounter_keys.sort();

    for key in changed_pncounter_keys {
        let Some(counter) = updates.pncounters.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_pncounter_state_key(key));
        set_values.push(counter.to_json()?.into_bytes());
        set_keys.push(materialized_pncounter_key(key));
        set_values.push(counter.value().to_le_bytes().to_vec());
    }

    let mut changed_lww_register_keys = updates.lww_registers.keys().collect::<Vec<_>>();
    changed_lww_register_keys.sort();

    for key in changed_lww_register_keys {
        let Some(register) = updates.lww_registers.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_lww_register_state_key(key));
        set_values.push(register.to_json()?.into_bytes());
        set_keys.push(materialized_lww_register_key(key));
        set_values.push(register.value_bytes());
    }

    let mut changed_lww_map_keys = updates.lww_maps.keys().collect::<Vec<_>>();
    changed_lww_map_keys.sort();

    for key in changed_lww_map_keys {
        let Some(map) = updates.lww_maps.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_lww_map_state_key(key));
        set_values.push(map.to_json()?.into_bytes());
        set_keys.push(materialized_lww_map_key(key));
        set_values.push(serde_json::to_vec(&map.entries())?);
    }

    let mut changed_orset_keys = updates.orsets.keys().collect::<Vec<_>>();
    changed_orset_keys.sort();

    for key in changed_orset_keys {
        let Some(set) = updates.orsets.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_orset_state_key(key));
        set_values.push(set.to_json()?.into_bytes());
        set_keys.push(materialized_orset_key(key));
        set_values.push(serde_json::to_vec(&set.elements())?);
    }

    let mut changed_rga_keys = updates.rgas.keys().collect::<Vec<_>>();
    changed_rga_keys.sort();

    for key in changed_rga_keys {
        let Some(rga) = updates.rgas.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_rga_state_key(key));
        set_values.push(rga.to_json()?.into_bytes());
        set_keys.push(materialized_rga_key(key));
        set_values.push(serde_json::to_vec(&rga.values())?);
    }

    for plan in plans {
        set_keys.push(seen_op_store_key(plan.op.id.as_str()));
        set_values.push(plan.seen_sequence.to_be_bytes().to_vec());
        set_keys.push(op_log_store_key(plan.op.id.as_str()));
        set_values.push(encode_durable_op_log_value(plan.op_log_sequence, &plan.op)?);
        delete_keys.extend(collect_seen_delete_keys(&plan.seen_evicted));
        delete_keys.extend(collect_op_log_delete_keys(&plan.op_log_evicted));
    }

    let sets = set_keys
        .iter()
        .zip(set_values.iter())
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    let deletes = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();

    store.apply_batch(&sets, &deletes)?;
    Ok(())
}
