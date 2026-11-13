use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use std::sync::Arc;

use nx_core::{
    SyncConfig,
    observability::RuntimeMetrics,
    sync_manager::{MigrationOptions, SyncManager, migrate_sync_schema},
};
use nx_store::Store;
use nx_sync::{GCounter, LwwMap, LwwRegister, NodeId, ORSet, Op, OpId, PNCounter, Rga};

const SOURCE_TAG: &str = "v0.1.5";
const SOURCE_COMMIT: &str = "471a2281d0ffc8b7a3c20550320854812b8af726";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: generator <output-file>")?;
    let database = tempfile::tempdir()?;
    let store = Arc::new(Store::open(database.path())?);
    let node_a = NodeId::new("fixture-node-a");
    let node_b = NodeId::new("fixture-node-b");

    let mut gcounter = GCounter::new();
    gcounter.increment(&node_a, 12);
    write_state_pair(
        &store,
        "__nx/crdt/gcounter/visits",
        &gcounter.value().to_le_bytes(),
        "__nx/crdt/state/gcounter/visits",
        gcounter.to_json()?.as_bytes(),
    )?;

    let mut pncounter = PNCounter::new();
    pncounter.increment(&node_a, 10);
    pncounter.decrement(&node_b, 4);
    write_state_pair(
        &store,
        "__nx/crdt/pncounter/stock",
        &pncounter.value().to_le_bytes(),
        "__nx/crdt/state/pncounter/stock",
        pncounter.to_json()?.as_bytes(),
    )?;

    let register = LwwRegister::new(b"online".to_vec(), 1_700_000_000_100, node_a.clone());
    write_state_pair(
        &store,
        "__nx/crdt/lww-register/status",
        register.value(),
        "__nx/crdt/state/lww-register/status",
        register.to_json()?.as_bytes(),
    )?;

    let mut map = LwwMap::new();
    map.set("region", b"eu".to_vec(), 1_700_000_000_200, node_a.clone());
    map.set(
        "obsolete",
        b"old".to_vec(),
        1_700_000_000_201,
        node_a.clone(),
    );
    map.remove("obsolete", 1_700_000_000_202, node_b.clone());
    write_state_pair(
        &store,
        "__nx/crdt/lww-map/settings",
        &serde_json_bytes(&map.entries())?,
        "__nx/crdt/state/lww-map/settings",
        map.to_json()?.as_bytes(),
    )?;

    let mut set = ORSet::new();
    set.add("blue", "tag-blue");
    set.add("removed", "tag-removed");
    set.remove("removed");
    write_state_pair(
        &store,
        "__nx/crdt/orset/tags",
        &serde_json_bytes(&set.elements())?,
        "__nx/crdt/state/orset/tags",
        set.to_json()?.as_bytes(),
    )?;

    let mut rga = Rga::new();
    rga.insert("element-a", None::<String>, b"hello".to_vec());
    rga.insert("element-b", Some("element-a"), b"removed".to_vec());
    rga.delete("element-b");
    write_state_pair(
        &store,
        "__nx/crdt/rga/document",
        &serde_json_bytes(&rga.values())?,
        "__nx/crdt/state/rga/document",
        rga.to_json()?.as_bytes(),
    )?;

    // Deterministic identities make the logical fixture independently reproducible.
    // These are retained historical operations, not a complete replayable history.
    let operations = [
        Op::gcounter_increment(node_a.clone(), "visits", 3),
        Op::pncounter_increment(node_a.clone(), "stock", 10),
        Op::lww_register_set(
            node_a.clone(),
            "status",
            b"online".to_vec(),
            1_700_000_000_100,
        ),
        Op::lww_map_set(
            node_a.clone(),
            "settings",
            "region",
            b"eu".to_vec(),
            1_700_000_000_200,
        ),
        Op::orset_add(node_a.clone(), "tags", "blue", "tag-blue"),
        Op::rga_insert(
            node_a.clone(),
            "document",
            "element-a",
            None::<String>,
            b"hello".to_vec(),
        ),
    ];
    for (index, mut op) in operations.into_iter().enumerate() {
        op.id = OpId::new(format!("fixture-op-{}", index + 1));
        let sequence = (index as u64 + 1).to_be_bytes();
        store.set(
            format!("__nx/crdt/seen-op/{}", op.id.as_str()).as_bytes(),
            &sequence,
        )?;
        let mut value = sequence.to_vec();
        value.extend_from_slice(&op.to_bytes()?);
        store.set(
            format!("__nx/crdt/op-log/{}", op.id.as_str()).as_bytes(),
            &value,
        )?;
    }
    store.set(b"application/untouched", b"application-value")?;
    // Use the release runtime itself to initialize and validate all schema headers
    // and hydrate all six families. Do not synthesize current schema metadata.
    migrate_sync_schema(&store, MigrationOptions::default())?;
    let _manager = SyncManager::try_new(
        node_a,
        SyncConfig::new(),
        Arc::clone(&store),
        Arc::new(RuntimeMetrics::default()),
    )?;
    store.flush()?;

    let records = store.scan_prefix(b"")?;
    let mut fixture = format!(
        "# Numax persistence fixture\n# source-tag: {SOURCE_TAG}\n# source-commit: {SOURCE_COMMIT}\n"
    );
    for (key, value) in records {
        writeln!(&mut fixture, "{}\t{}", encode_hex(&key), encode_hex(&value))?;
    }
    fs::write(output, fixture)?;
    Ok(())
}

fn write_state_pair(
    store: &Store,
    materialized_key: &str,
    materialized_value: &[u8],
    state_key: &str,
    state_value: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    store.set(materialized_key.as_bytes(), materialized_value)?;
    let state: serde_json::Value = serde_json::from_slice(state_value)?;
    store.set(state_key.as_bytes(), &serde_json::to_vec(&state)?)?;
    Ok(())
}

fn serde_json_bytes<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(value)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
