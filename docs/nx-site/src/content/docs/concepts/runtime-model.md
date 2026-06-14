---
title: Runtime model
description: How Numax executes modules, manages local state and the philosophy behind the runtime model.
---

The Numax runtime model is built on one premise: **a distributed application should not need a distributed infrastructure to work**.

Every capability a module needs: compute, state, synchronization lives in the same process, on the same node. There is no remote database to query, no broker to route through, no orchestrator to consult. The node is self-sufficient.

---

## Three components, and only three

Numax integrates three things, and deliberately nothing more:

```
 ┌──────────────────────────────────────────┐
 │           WASM module (guest)            │
 │        compiled with nx-sdk              │
 └─────────────────┬────────────────────────┘
                   │  Host API (namespace "nx")
                   ▼
 ┌──────────────────────────────────────────┐
 │              nx-core (host)              │
 │  ┌──────────┐  ┌──────────┐  ┌────────┐ │
 │  │ Wasmtime │  │ Host API │  │  WASI  │ │
 │  └──────────┘  └────┬─────┘  └────────┘ │
 └───────────────────┬─┼────────────────────┘
                     │ │
          ┌──────────┘ └──────────┐
          ▼                       ▼
 ┌────────────────┐     ┌──────────────────────┐
 │   nx-store     │     │  nx-sync + nx-net     │
 │  sled (local)  │◄────┤  CRDT + gossip + TLS  │
 └────────────────┘     └──────────────────────┘
```

**1. Execution** - a WASM module runs in a sandbox. It has no access to the filesystem, network or system resources except what the host explicitly exposes via the `nx` namespace. Isolation is structural, not configured.

**2. Local state** - an embedded sled database lives inside the runtime. Reads and writes are local, with no network hop, and sled provides transactional guarantees for individual operations. There is no connection to open. The store is just there.

**3. Distributed synchronization** - when sync is enabled, a subset of state is replicated between nodes using CRDTs and a gossip protocol. This is opt-in: a node without `--listen` works perfectly as a standalone node. When sync is on, convergence is mathematically guaranteed by the CRDT properties, not by locks or consensus.

---

## The host/guest boundary

The module and the runtime live in different worlds. The module is a `.wasm` binary: a portable, sandboxed, architecture-independent unit of computation. The runtime is a native Rust process. They communicate through the Host API.

Every Host API function follows the same convention:

- pointers and lengths are passed as `u32` offsets into WASM linear memory
- return codes are `i32`: non-negative values mean success, negative values carry error codes
- the guest handles errors deterministically

```
Guest code (Rust, via nx-sdk)
    │
    │  safe wrapper (e.g. db::set("key", b"value"))
    ▼
FFI call  (unsafe extern "C" from ffi.rs)
    │
    │  WASM linear memory boundary
    ▼
Host function (Rust in nx-core)
    │
    │  writes to sled / pushes to SyncManager / reads from clock
    ▼
Return code (i32) back to guest
```

| Code | Meaning |
|---|---|
| `>= 0` | success; depending on the function, the value is a byte count, boolean/status value, or zero |
| `-1` | key not found |
| `-2` | output buffer too small, retry |
| `-3` | internal error |
| `-4` | reserved key (`__nx/` prefix) |
| `-5` | sync disabled on this runtime |

The SDK handles the `-2` retry loop automatically by growing its output buffer and retrying.
Module code does not need to manage buffer sizing by hand.

---

## Module lifecycle

A Numax node follows a fixed lifecycle:

```
Runtime::new(config)          open store, build engine, register host API, create SyncManager if sync is configured
  start_observability()       optionally bind HTTP metrics endpoint
  start_sync()                optionally bind TCP listener and dial configured peers
  wait_before_run(duration)   optionally retry peer connections before running
  run_module(wasm_bytes)      compile, instantiate, call run() or _start()
  settle_for(duration)        optionally keep sync alive for a bounded window
    OR serve()                if sync is enabled and no settle window is set, keep alive until OS signal
  shutdown_with_timeout()     stop sync, flush store, close connections
```

The module itself is stateless from the runtime's perspective. It receives control, calls Host API functions as needed, and returns.
What persists is in the store and in the CRDT registry, not in the module.

Compiled modules are cached by the blake3 hash of their bytes. Running the same module repeatedly does not pay compilation cost again.

---

## Local state: the store

The local key/value store is a sled embedded database. It opens from a directory on disk and persists between restarts, and each node owns its own store directory.

Keys are arbitrary byte slices and values are arbitrary byte slices.

The runtime reserves the `__nx/` key prefix for its own internal state: NodeId, materialized CRDT values, op-log entries. Guest code that attempts to read or write a reserved key receives `ERR_RESERVED_KEY`.

On shutdown, the runtime calls an explicit `flush()` to ensure all writes reach disk before the process exits.

---

## Distributed synchronization: CRDTs

When sync is enabled, guest modules can operate on CRDT data structures via the Host API. A CRDT operation call from the guest:

1. is applied immediately to the in-memory CRDT state
2. persists the updated CRDT state/materialized value in sled
3. queues the generated op for broadcast
4. is recorded in the seen-op set and op-log by the broadcast loop before network send
5. is deduplicated by OpId when received from a peer

The sync manager in `nx-core` owns the CRDT registry. It bridges the host API, the local store and the network layer. It does not expose a query language: the guest calls typed functions (`crdt_gcounter_inc`, `crdt_lww_set`, etc.) and the runtime takes care of everything else.

Available CRDTs:

| Type | Use for |
|---|---|
| GCounter | totals that only grow |
| PNCounter | counters that can decrease |
| LWW-Register | single values with last-writer-wins |
| LWW-Map | maps where each field is independent LWW |
| ORSet | sets with concurrent add and remove |
| RGA | ordered sequences |

CRDTs satisfy three properties that make distributed convergence possible without coordination:

- **Commutativity** - `merge(A, B) == merge(B, A)`: order of arrival does not matter
- **Associativity** - `merge(merge(A, B), C) == merge(A, merge(B, C))`: grouping does not matter
- **Idempotency** - `merge(A, A) == A`: receiving the same op twice does not corrupt state

A node can go offline, receive operations in any order, reconnect after days, and still converge to the same state as every other node. It is a mathematical property. (How cool is that!!)

---

## NodeId and identity

Every Numax node has a `NodeId`: an opaque string that uniquely identifies it.

Without TLS, the runtime generates a random UUID v4 on first start and persists it under `__nx/runtime/node_id`. The same identity is reused on every subsequent start from the same store directory.

With TLS, the NodeId is derived from the SHA-256 of the `SubjectPublicKeyInfo` bytes of the node's X.509 certificate: first 16 hash bytes, encoded as a 32-character lowercase hex string. A node's identity is its key. It cannot be forged without the private key.

The NodeId is used:
- as a slot key in CRDT state (each node's GCounter slot is keyed by NodeId)
- as the `origin` field in every `Op`
- for mTLS peer identity verification and allowlist enforcement

---

## What the runtime is not

Understanding the model is also understanding what is deliberately absent.

**No central coordinator.** There is no leader election, no primary node, no consensus round. Nodes are peers. Any node can accept writes. Convergence comes from CRDT properties, not from a coordinator. But that was probably already clear if you are here.

**No remote state.** The store is embedded, not a service. You do not open a connection. There is no network hop between compute and state.

**No runtime configuration language.** A node is configured with a TOML file and CLI flags. One binary, one config file, one `.wasm` module.

**No implicit networking.** A node without `--listen` does not open any port, does not connect to any peer, does not replicate anything. Sync is fully opt-in.

---

## The philosophy

The Numax runtime model makes a deliberate trade:

It gives up generality and does not try to be a universal distributed systems toolkit, but in exchange a coherent, minimal model where the hard parts (isolation, persistence, convergence) are solved structurally.

The goal is not to eliminate the complexity of distributed systems. That complexity is real and cannot be wished away. The goal is to eliminate the *self-imposed* complexity: the layers of tools, configurations and infrastructure that accumulate not because the problem requires them, but because that is what the existing ecosystem assumes you will use.

A Numax node is a single process. You ship a `.wasm` file. You run it. The rest is already there.

---

## Related

- [CRDT and state](/numax/concepts/crdt-and-state/) - deep dive on the CRDT model
- [WASM execution](/numax/concepts/wasm-execution/) - how modules are compiled and instantiated
- [Gossip protocol](/numax/concepts/gossip-protocol/) - how operations propagate between nodes
- [Host API](/numax/reference/host-api/) - every function the guest can call
- [nx-core crate](/numax/reference/crates/nx-core/) - `Runtime` and `SyncManager` internals
