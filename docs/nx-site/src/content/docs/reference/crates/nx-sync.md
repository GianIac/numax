---
title: nx-sync
description: CRDT types, operations and node identifiers.
---

`nx-sync` is the pure-logic core of the Numax replication model.
It owns CRDT data structures, operation types, node identifiers, and serialization.
It has no I/O, no async, no networking, no persistence. It does not depend on any other
crate in the workspace.

This is the one crate you can reason about, test, and verify in isolation.
If a CRDT merge is wrong, the bug is here. If an op is malformed, the bug is here.
Everything else just moves ops around.

---

## Responsibilities

| Responsibility | Where |
|---|---|
| `NodeId` type and generation | `node_id.rs` |
| `OpId` type (UUID v4) | `op.rs` |
| `OpKind` enum (one variant per operation) | `op.rs` |
| `Op` struct and builder methods | `op.rs` |
| JSON serialization for ops | `op.rs` `to_json`, `to_bytes`, `from_json`, `from_bytes` |
| GCounter state and merge | `crdt/gcounter.rs` |
| PNCounter state and merge | `crdt/pncounter.rs` |
| LWW-Register state and merge | `crdt/lww_register.rs` |
| LWW-Map state and merge | `crdt/lww_map.rs` |
| ORSet state and merge | `crdt/orset.rs` |
| RGA state and merge | `crdt/rga.rs` |
| Error types | `error.rs` |

---

## NodeId

`NodeId` is a newtype over `String`. It identifies a node in the cluster.

```rust
// from a fixed string (tests, config)
let id = NodeId::new("node-a");

// random UUID v4 (used by Runtime::new on first start without TLS)
let id = NodeId::generate();

id.as_str()  // &str
id.to_string() // String via Display
```

`NodeId` implements `Hash`, `Eq`, `Serialize`, `Deserialize`, `From<&str>`, `From<String>`.

In TLS mode, the NodeId is not random. It is derived from the SHA-256 of the
`SubjectPublicKeyInfo` bytes of the node's X.509 certificate: first 16 hash bytes,
encoded as a 32-character lowercase hex string.
This derivation lives in `nx-net/src/tls.rs` (`derive_protocol_node_id_from_cert`).
`nx-sync` itself only stores and compares NodeIds as opaque strings.

---

## OpId

`OpId` is a newtype over `String`. Every operation has a globally unique id.

```rust
let id = OpId::generate();  // UUID v4
let id = OpId::new("custom-id");  // from string (tests, replay)
id.as_str()  // &str
```

`OpId` implements `Hash`, `Eq`, `Serialize`, `Deserialize`, `Display`.

OpIds are used for deduplication in the sync manager. The seen-ops set in `nx-core`
tracks OpIds to avoid applying the same op twice. `nx-sync` itself does not enforce this.
The comment in `GCounter::apply_op` is explicit about it: callers must deduplicate before calling.

For ORSet and RGA, the OpId is also used as an element tag or element id:
`Op::orset_add_with_op_id_tag` and `Op::rga_insert_with_op_id` generate the element
identity from the op's own id, so ids are stable without a separate allocation.

---

## OpKind

`OpKind` is an enum with one variant per operation type. Every variant carries
the key and the data needed to apply the operation to the corresponding CRDT.

```rust
pub enum OpKind {
    GCounterIncrement  { key: String, increment: u64 },
    PNCounterIncrement { key: String, increment: u64 },
    PNCounterDecrement { key: String, decrement: u64 },
    LwwRegisterSet     { key: String, value: Vec<u8>, timestamp_ms: u64 },
    LwwMapSet          { key: String, field: String, value: Vec<u8>, timestamp_ms: u64 },
    LwwMapRemove       { key: String, field: String, timestamp_ms: u64 },
    ORSetAdd           { key: String, element: String, tag: String },
    ORSetRemove        { key: String, element: String, observed_tags: Vec<String> },
    RgaInsert          { key: String, id: String, parent: Option<String>, value: Vec<u8> },
    RgaDelete          { key: String, id: String },
}
```

Each CRDT's `apply_op` method matches on its own variants and returns `Ok(false)` for
all others. This is how a single op can be routed to the right CRDT without a dispatch table.

---

## Op

`Op` is the complete, wire-ready operation.

```rust
pub struct Op {
    pub id:     OpId,    // globally unique
    pub origin: NodeId,  // node that generated this op
    pub kind:   OpKind,  // type and payload
}
```

### Builder methods

```rust
Op::gcounter_increment(origin, "counter:visits", 1)
Op::pncounter_increment(origin, "inventory:sku-1", 10)
Op::pncounter_decrement(origin, "inventory:sku-1", 3)
Op::lww_register_set(origin, "status:user-1", b"online".to_vec(), timestamp_ms)
Op::lww_map_set(origin, "settings:svc-a", "theme", b"dark".to_vec(), timestamp_ms)
Op::lww_map_remove(origin, "settings:svc-a", "region", timestamp_ms)
Op::orset_add(origin, "tags:item-1", "blue", "explicit-tag")
Op::orset_add_with_op_id_tag(origin, "tags:item-1", "blue")   // tag = op.id
Op::orset_remove(origin, "tags:item-1", "blue", observed_tags)
Op::rga_insert(origin, "comments:doc-1", "elem-id", Some("parent-id"), b"text")
Op::rga_insert_with_op_id(origin, "comments:doc-1", Some("parent-id"), b"text")  // id = op.id
Op::rga_delete(origin, "comments:doc-1", "elem-id")
```

### Serialization

```rust
let json: String    = op.to_json()?;
let bytes: Vec<u8>  = op.to_bytes()?;   // JSON bytes
let op = Op::from_json(&json)?;
let op = Op::from_bytes(&bytes)?;
```

Both `to_bytes` and `from_bytes` use JSON under the hood (via `serde_json`).
Bincode serialization for wire transport is handled in `nx-net` using serde derive.

---

## CRDT properties

All CRDT implementations in this crate satisfy the three properties required for
conflict-free merge:

| Property | Meaning | How verified |
|---|---|---|
| Commutativity | `merge(a, b) == merge(b, a)` | test: `merge_commutativity` in each CRDT |
| Associativity | `merge(merge(a,b),c) == merge(a,merge(b,c))` | test: `merge_associativity` in each CRDT |
| Idempotency | `merge(a, a) == a` | test: `merge_idempotency` in each CRDT |

These properties guarantee that any node can receive ops in any order, any number of times,
and converge to the same state as every other node.

---

## GCounter

Grow-only counter. Each node owns its own `u64` slot. The converged value is the sum.

```
state: HashMap<NodeId, u64>
merge: take max of each slot
value: sum of all slots
```

```rust
let mut c = GCounter::new();
c.increment(&node, 5);
let total: u64 = c.value();
let node_slot: u64 = c.value_for(&node);

c.merge(&other);
let merged = c.merged_with(&other);
let changed: bool = c.apply_op(&op)?;

let json = c.to_json()?;
let c2 = GCounter::from_json(&json)?;
```

Slot values use `saturating_add` to prevent overflow. Merging takes `max` per slot,
so an old state from a restarted node can never decrease another node's counter.

`apply_op` is not idempotent. Applying the same `OpId` twice increments twice.
The sync manager deduplicates by OpId before calling.

---

## PNCounter

Positive/negative counter. Internally two `GCounter`s: one for increments, one for decrements.
The converged value is `sum(positive) - sum(negative)`, clamped to `i64`.

```
state: positive: GCounter, negative: GCounter
merge: merge both GCounters independently
value: positive.sum() - negative.sum()  (clamped to i64)
```

```rust
let mut c = PNCounter::new();
c.increment(&node, 10);
c.decrement(&node, 3);
let v: i64 = c.value();          // 7
let pos: u64 = c.positive_for(&node);
let neg: u64 = c.negative_for(&node);
```

Slot overflow saturates: if `u64::MAX` is incremented by 1, the slot stays at `u64::MAX`.
The final `i64` value is clamped to `[i64::MIN, i64::MAX]` via `clamp_i128_to_i64`.

---

## LwwRegister

Last-writer-wins register. Stores a single `(value, timestamp_ms, writer)` triple.

```
state: value: Vec<u8>, timestamp_ms: u64, writer: NodeId
merge: keep the entry that wins the LWW ordering
```

**Conflict resolution order:**
1. Higher `timestamp_ms` wins.
2. Equal timestamps: lexicographically greater `NodeId` string wins.

This is deterministic and requires no coordination. Both sides of a merge always
pick the same winner.

```rust
let mut r = LwwRegister::new(b"online".to_vec(), 100, NodeId::new("node-a"));
let changed: bool = r.merge(&other);
let changed: bool = r.assign(b"away".to_vec(), 200, NodeId::new("node-b"));

r.value()        // &[u8]
r.value_bytes()  // Vec<u8>
r.timestamp_ms() // u64
r.writer()       // &NodeId
```

`assign` applies the same LWW ordering as `merge`. It is used by the host API
when the guest calls `crdt_lww_set` to ensure local writes are monotone against
the currently observed state.

---

## LwwMap

A map where each field follows LWW-register semantics. Removes are stored as tombstones
(a remove is a `LwwMapRemove` op with a timestamp, applied as a winning entry whose value is `None`).

```
state: BTreeMap<field, LwwMapEntry { value: Option<Vec<u8>>, timestamp_ms, writer }>
merge: per-field LWW
```

Tombstoned fields are not returned by `entries()`. An old add replayed after a remove
cannot resurrect a tombstoned field if the remove's timestamp is higher.

```rust
lww_map.set("theme", b"dark", timestamp_ms, writer);
lww_map.remove("region", timestamp_ms, writer);
let val: Option<&[u8]>           = lww_map.get("theme");
let exists: bool                  = lww_map.contains("theme");
let all: Vec<(String, Vec<u8>)>   = lww_map.entries(); // visible only
lww_map.merge(&other);
```

---

## ORSet

Observed-remove set of strings. State is two maps: `adds` and `removes`, both `BTreeMap<element, BTreeSet<tag>>`.

```
state: adds: BTreeMap<String, BTreeSet<String>>,
       removes: BTreeMap<String, BTreeSet<String>>
merge: union both maps
visible: element is visible iff it has at least one add-tag not in removes
```

A remove carries the add-tags observed locally for that element. Concurrent adds
that used different tags and were not observed remain visible after merge.

```rust
let mut s = ORSet::new();
s.add("blue", "tag-1");
s.add("blue", "tag-2");
let observed = s.remove("blue");   // returns ["tag-1", "tag-2"]
s.apply_remove("blue", observed);
let visible: bool         = s.contains("blue");
let all: Vec<String>      = s.elements(); // sorted (BTreeMap ordering)
let tags: Vec<String>     = s.observed_tags("blue");
s.merge(&other);
```

`elements()` returns elements in deterministic BTree order. `add` returns `false`
if the tag was already present (idempotent for the same tag).

---

## RGA

Replicated Growable Array. Ordered sequence of byte values. Each element has a stable `String` id
and an optional parent id. Deletes are stored in a tombstone set by id; children of deleted elements remain visible.

```
state: elements: BTreeMap<RgaElementId, RgaElement { id, parent, value }>
       tombstones: BTreeSet<RgaElementId>
insert: add a globally unique element id with an optional parent
delete: add the element id to the tombstone set
values: visible elements in sequence order, tombstones excluded
```

```rust
let inserted = rga.insert("elem-1", None::<String>, b"first");          // head insert
let inserted = rga.insert("elem-2", Some("elem-1"), b"reply");          // insert after first
rga.delete("elem-2");

let visible: Vec<Vec<u8>> = rga.values();
rga.merge(&other);
```

The RGA merge resolves concurrent inserts at the same parent position deterministically
using the element id as a tiebreaker, ensuring all nodes converge to the same sequence.

---

## Serialization

All CRDT structs and the `Op` type derive `Serialize`/`Deserialize` via serde.

For ops, `nx-sync` uses JSON (`serde_json`) for its own `to_json`/`from_json` helpers.
Wire transport in `nx-net` uses either bincode or JSON via serde derive directly.
The two serialization paths are independent and both are tested.

State persistence for CRDTs (durable to `nx-store`) also goes through JSON,
managed by the sync manager in `nx-core`.

---

## Error types

```rust
pub enum SyncError {
    Serialization(serde_json::Error),  // JSON parse/serialize failure
    UnknownNode(String),
    DuplicateOp(String),
    InvalidOp(String),
}
```

---

## How to add a new CRDT (developer guide)

1. Add the state struct in a new file `crdt/my_crdt.rs`.
2. Implement `new`, `apply_op`, `merge`, `merged_with`, `to_json`, `from_json`.
3. Add the new `OpKind` variants to `op.rs`.
4. Add builder methods to `Op` in `op.rs`.
5. Add `pub mod my_crdt` to `crdt/mod.rs` and re-export from `lib.rs`.
6. Add the CRDT to the registry in `nx-core/src/sync_manager.rs`.
7. Add host API functions in `nx-core/src/host_api/crdt.rs`.
8. Add SDK wrappers in `nx-sdk/src/crdt/my_crdt.rs`.
9. Write tests: commutativity, associativity, idempotency, `apply_op`, JSON roundtrip.
10. Write an E2E test in `sync_manager.rs`.
11. Write a distributed example in `examples/distributed_my_crdt/`.
12. Check the completion rule from the Host API docs before marking it done.

---

## Test coverage

Tests live inside each source file in `#[cfg(test)]` blocks.

| Test group | What it covers |
|---|---|
| `node_id` | unique generation, from-string, serde roundtrip |
| `op` | all builder constructors, JSON/bytes roundtrip for every OpKind |
| `gcounter` | zero init, increment, multiple nodes, merge (max), commutativity, associativity, idempotency, apply_op, JSON roundtrip, overflow saturation |
| `pncounter` | zero init, inc+dec, negative value, multiple nodes, merge (max slots), commutativity, associativity, idempotency, apply inc/dec ops, ignore wrong op kind, overflow saturation, i64 clamping |
| `lww_register` | new, merge newer wins, merge older ignored, equal-timestamp NodeId tiebreaker, assign ordering, commutativity, associativity, idempotency, JSON roundtrip |
| `lww_map` | set, remove, get, contains, entries (tombstones excluded), merge, properties |
| `orset` | empty, add visible, duplicate tag idempotent, remove hides tags, remove of unseen, concurrent add survives observed-remove, full-observe remove hides, commutativity, associativity, idempotency, JSON roundtrip |
| `rga` | insert at head, insert after, delete, values order, merge, properties |

```bash
cargo test -p nx-sync
```

---

## Related

Use this page together with the layers that create, persist and transport sync operations:

- [Crates overview](/numax/reference/crates/) - where `nx-sync` sits in the dependency graph
- [nx-core crate](/numax/reference/crates/nx-core/) - the `SyncManager` that applies, deduplicates and persists ops
- [nx-net crate](/numax/reference/crates/nx-net/) - wire transport for `Op`
- [Host API](/numax/reference/host-api/) - guest-facing functions that create CRDT ops
