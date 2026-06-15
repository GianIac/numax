---
title: CRDT and state
description: How replicated state converges across Numax nodes.
---

This page explains what CRDTs are, why they exist, how Numax uses them, and what each type is for. By the end you will know exactly which one to reach for and why convergence is guaranteed without coordination.

---

## The problem CRDTs solve

Imagine two nodes, each running your module. Both offline. Both accepting writes. When they reconnect, their states are different.

With a traditional database, you need to pick a winner, run a merge procedure, or reject one of the writes. Someone has to coordinate. Coordination requires connectivity. And if the network is unreliable, your application is unreliable.

CRDTs eliminate this problem entirely. A **Conflict-free Replicated Data Type** is a data structure designed so that any two copies, regardless of how different they are, can always be merged into one consistent result — automatically, without asking anyone for permission, without a coordinator and without a lock.

---

## The three properties that make it work

Every CRDT in Numax satisfies three fundamental mathematical properties which are:

**Commutativity.** The order in which you apply updates does not matter.

```
merge(A, B) == merge(B, A)
```

Node 1 receives the op from Node 2, then the one from Node 3.
Node 2 receives the op from Node 3, then the one from Node 1.
They converge to the same state.

**Associativity.** How you group the merges does not matter.

```
merge(merge(A, B), C) == merge(A, merge(B, C))
```

You can merge in batches, in parallel, incrementally. The result is always the same.

**Idempotency.** Merging the same state twice does not change anything.

```
merge(A, A) == A
```

If the same state is merged twice, the state does not corrupt. Operation application is handled separately: replicated ops are deduplicated by `OpId` before they are applied.

These three properties together mean: **any node can merge replicated state in any order and converge to the same state as every other node that has seen the same updates.**

All three are explicitly tested in `nx-sync`:

```bash
cargo test -p nx-sync
# test_gcounter_merge_commutativity
# test_gcounter_merge_associativity
# test_gcounter_merge_idempotency
```

---

## How ops flow through the system

When a guest module calls a CRDT host function, here is what happens:

```
guest calls crdt_gcounter_inc("visits", 1)
       │
       ▼
host validates input, reads NodeId from HostState
       │
       ▼
Host API applies op to in-memory CRDT state
       │
       ├── persists updated CRDT state / materialized value in sled (under __nx/)
       │
       └── queues op for broadcast
              │
              ▼
         broadcast loop records seen-op + op-log metadata
              │
              ▼
         nx-net sends PushOps to each peer
              │
              ▼
         peer receives PushOps, checks OpId against seen-ops set
              │
              ├── if already seen: discard
              │
              └── if new: apply to peer's in-memory CRDT state
                          persist updated state and op-log metadata
                          mark OpId as seen
```

The module never waits for peers — the only thing it waits for is the push into the sync manager's channel. Propagation to peers happens asynchronously in the background.

---

## Op-log and deduplication

Every CRDT operation is an `Op`:

```rust
pub struct Op {
    pub id:     OpId,    // UUID v4, globally unique
    pub origin: NodeId,  // node that generated this op
    pub kind:   OpKind,  // which CRDT, which key, what data
}
```

The op-log is a bounded list of `Op` values persisted in sled under `__nx/crdt/op-log/`. It is used for anti-entropy and catch-up between peers. CRDT state itself is also persisted under per-type `__nx/crdt/state/...` keys, with some materialized values kept under older `__nx/crdt/...` prefixes for compatibility. On restart, the runtime hydrates the in-memory CRDT registry from those durable state keys, so CRDT state survives process restarts even when the op-log is bounded.

The seen-ops set is a bounded `HashSet<OpId>` (capped at `seen_ops_limit`, default 100,000). Before applying a remote op, the sync manager checks whether its `OpId` is already in the set. If yes: discard. If no: apply and add to the set.

This is the mechanism that makes operation delivery practical: the mathematical merge properties cover state convergence, and the seen-ops set prevents applying non-idempotent operation deltas more than once.

---

## The six CRDTs

Numax ships six CRDT types. Each solves a specific problem. Here they are with no ambiguity about when to use which.

---

### GCounter - the grow-only counter

**Use it for:** totals that can only increase. Page views, event counts, likes, completed tasks, bytes transferred.

**How it works:** each node owns its own `u64` slot. A node can only increment its own slot. The total is the sum of all slots. Merge takes the maximum of each slot.

```
Node A slot: 10
Node B slot:  7
             ---
Total:        17
```

If Node A's slot on Node B shows 8 but Node A actually has 10, merge will use 10. A slot can never decrease. This makes concurrent increments safe without coordination.

```rust
use nx_sdk::crdt::gcounter;

gcounter::inc("counter:visits", 1)?;
let total: u64 = gcounter::value("counter:visits")?;
```

---

### PNCounter - the counter that goes both ways

**Use it for:** anything that increases and decreases. Inventory levels, account balances, active sessions, temperature readings as deltas.

**How it works:** two GCounters internally. One tracks increments (`P`), one tracks decrements (`N`). The visible value is `P - N`. Merge handles each GCounter independently.

```
Node A local slots: P[A]=10, N[A]=3  ->  value = 7
Node B local slots: P[B]=5,  N[B]=8  ->  value = -3
After merge:
  P slots = { A: 10, B: 5  } -> sum P = 15
  N slots = { A: 3,  B: 8  } -> sum N = 11
  value = 15 - 11 = 4
```

Overflow is handled with `saturating_add` and the final `i64` value is clamped to `[i64::MIN, i64::MAX]`.

```rust
use nx_sdk::crdt::pncounter;

pncounter::inc("inventory:sku-1", 10)?;
pncounter::dec("inventory:sku-1", 3)?;
let available: i64 = pncounter::value("inventory:sku-1")?;
```

---

### LWW-Register - last writer wins

**Use it for:** a single value per key where the latest write should win. User status, configuration settings, current location, feature flags.

**How it works:** the register stores `(value, timestamp_ms, writer_node_id)`. When two writes conflict, the one with the higher timestamp wins. If timestamps are equal, the lexicographically greater NodeId wins. This is deterministic: both sides always pick the same winner.

```
Node A writes "online" at t=100
Node B writes "away"   at t=150
After merge: "away"  (t=150 wins)

Node A writes "online" at t=100
Node B writes "away"   at t=100
After merge: winner is the node with lexicographically greater NodeId
```

The tie-breaker is why clocks do not need to be perfectly synchronized. Even if two writes happen at the exact same millisecond, the result is deterministic.

```rust
use nx_sdk::crdt::lww_register;

lww_register::set("status:user-1", b"online")?;
let status: Option<Vec<u8>> = lww_register::get("status:user-1")?;
```

---

### LWW-Map - a map of independent LWW-registers

**Use it for:** a set of named settings or properties where each field evolves independently. Service configuration, user preferences, metadata maps.

**How it works:** each field in the map is its own LWW-register. Fields are merged independently. Removes are tombstones: a `remove` at timestamp `t` wins over any `set` at timestamp `< t`, but loses to a `set` at timestamp `> t`. A removed field can be resurrected by a later write.

```
Node A: { "theme": "dark" at t=100 }
Node B: { "theme": "light" at t=200, "region": "eu" at t=100 }
After merge: { "theme": "light", "region": "eu" }

Node A: { "theme": "dark" at t=100 }
Node B: removes "theme" at t=200
After merge: "theme" is gone (tombstoned)
```

```rust
use nx_sdk::crdt::lww_map;

lww_map::set("settings:svc-a", "theme", b"dark")?;
lww_map::remove("settings:svc-a", "region")?;
let val: Option<Vec<u8>>       = lww_map::get("settings:svc-a", "theme")?;
let all: Vec<(String, Vec<u8>)> = lww_map::entries("settings:svc-a")?;
```

---

### ORSet - the set that handles concurrent removes correctly

**Use it for:** sets of strings where elements can be added and removed concurrently by different nodes. Active tags, connected devices, selected options, user roles.

**Why not just a flag?** A simple boolean "present/removed" flag breaks under concurrent operations. If Node A adds "blue" and Node B removes "blue" at the same time, and the remove wins, Node A's add is silently lost. That is not correct.

**How it works:** each `add` carries a unique tag (the OpId). A `remove` carries the set of tags it observed for that element. Merge unions both adds and removes. An element is visible if it has at least one add-tag that has not been removed.

```
Node A adds "blue" with tag "op-1"
Node B adds "blue" with tag "op-2"
Node B removes "blue", observing only "op-1"
After merge: "blue" is still visible (tag "op-2" was not removed)

Node A adds "blue" with tag "op-1"
Node A removes "blue", observing "op-1"
After merge: "blue" is gone (all tags removed)
```

Concurrent adds by different nodes always survive a remove that did not observe them. This is the "observed-remove" in the name.

```rust
use nx_sdk::crdt::orset;

orset::add("tags:item-1", "blue")?;
orset::remove("tags:item-1", "blue")?;
let has_blue: bool        = orset::contains("tags:item-1", "blue")?;
let all_tags: Vec<String> = orset::elements("tags:item-1")?;
```

---

### RGA - the ordered sequence

**Use it for:** ordered lists where concurrent insertions at the same position must converge to the same order. Collaborative documents, ordered queues, comment threads, log entries.

**How it works:** each element has a stable globally unique `id` and an optional `parent_id` (the element it was inserted after). The sequence is rebuilt by following parent links. When two inserts happen at the same position (same parent), the element with the lexicographically smaller `id` comes first. This is deterministic regardless of arrival order.

Deletes are tombstones: the element id is added to a deleted-ids set. The element stays in the data structure (so parent links remain valid) but is invisible in `values()`. Children of deleted elements remain visible.

```
insert "a" at head -> id="op-1"
insert "b" after "op-1" -> id="op-2"
insert "c" after "op-2" -> id="op-3"
values: ["a", "b", "c"]

delete "op-2"
values: ["a", "c"]
ordered_ids (including tombstones): ["op-1", "op-2", "op-3"]
```

```rust
use nx_sdk::crdt::rga;

let id1 = rga::insert_after("comments:doc-1", None, b"first comment")?;
let id2 = rga::insert_after("comments:doc-1", Some(&id1), b"reply")?;
rga::delete("comments:doc-1", &id2)?;
let visible: Vec<Vec<u8>> = rga::values("comments:doc-1")?;
```

---

## Which one to use

| Situation | CRDT |
|---|---|
| Count things, never remove | GCounter |
| Count things, can go up or down | PNCounter |
| Store one value, latest write wins | LWW-Register |
| Store many named values, each independently | LWW-Map |
| Maintain a set, concurrent add/remove | ORSet |
| Maintain an ordered list, concurrent inserts | RGA |

When in doubt: if the values in your data structure are independent of each other, LWW is almost always the right answer. If you need a collection where membership matters under concurrent operations, ORSet. If order matters, RGA.

---

## What CRDTs do not solve

CRDTs guarantee convergence of state, not correctness of application logic.

If your module reads a GCounter value and makes a business decision based on it, that decision is only as current as the last time sync ran. CRDTs do not give you linearizability or strong consistency. They give you **eventual consistency**: given enough time and connectivity, all nodes converge.

If you need strong guarantees like "this value must be exactly X before I proceed", CRDTs are not the right primitive. Use the local store for that, combined with whatever coordination mechanism your application needs.

---

## Related

- [Runtime model](/numax/concepts/runtime-model/) - how CRDT ops flow through the system
- [Gossip protocol](/numax/concepts/gossip-protocol/) - how ops propagate between nodes
- [nx-sync crate](/numax/reference/crates/nx-sync/) - CRDT implementations and mathematical properties
- [Host API](/numax/reference/host-api/) - all 19 CRDT host functions
- [nx-sdk CRDT wrappers](/numax/reference/crates/nx-sdk/) - the guest-side API
