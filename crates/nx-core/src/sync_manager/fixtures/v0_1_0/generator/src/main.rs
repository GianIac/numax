use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use nx_store::Store;
use nx_sync::{GCounter, LwwMap, LwwRegister, NodeId, ORSet, Op, OpId, OpKind, PNCounter, Rga};

const SOURCE_TAG: &str = "v0.1.0";
const SOURCE_COMMIT: &str = "9f4753b8d0706b069988487ac7f6e3939f6e9dbc";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: generator <output-file>")?;
    let database = tempfile::tempdir()?;
    let store = Store::open(database.path())?;
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

    let op = Op {
        id: OpId::new("fixture-op-1"),
        origin: node_a,
        kind: OpKind::GCounterIncrement {
            key: "visits".to_string(),
            increment: 3,
        },
    };
    store.set(b"__nx/crdt/seen-op/fixture-op-1", &7u64.to_be_bytes())?;
    let mut op_log_value = 9u64.to_be_bytes().to_vec();
    op_log_value.extend_from_slice(&op.to_bytes()?);
    store.set(b"__nx/crdt/op-log/fixture-op-1", &op_log_value)?;
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
) -> Result<(), nx_store::StoreError> {
    store.set(materialized_key.as_bytes(), materialized_value)?;
    store.set(state_key.as_bytes(), state_value)
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
