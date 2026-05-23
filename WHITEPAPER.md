# Numax Runtime - Technical Whitepaper

> **Note**
> This whitepaper is aligned with **v0.1.0-alpha.4**, the current public technical preview of Numax.
> Compared to previous drafts, most of the `TODO`s have been resolved based on the code present in the repository. What remains open is explicitly labeled as *(Planned)* and tracked in the roadmap.
>
> **Status labels (consistent with the code):**
> - **(Implemented)**: present in the code, working and covered by tests.
> - **(Prototype)**: partially present; internal wiring or critical paths already verified, but not yet production-ready.
> - **(Planned)**: foreseen in the roadmap, not yet implemented.
>
> **Version reference**: `v0.1.0-alpha.4` - technical preview, API and wire format may change before the stable v0.1.0.
>
> > 📍 **Reference roadmap:** the phases cited in this document (Phase 7, Phase 8, …) are defined in [`ROADMAP.md`](./ROADMAP.md).
> Whenever you read *Phase N*, you can consult the roadmap for details, completion criteria and progress status :)

---

## 1. Executive Summary

### 1.1 The problem

Building distributed applications, today, is disproportionately complex compared to what most of these applications actually do.

For any logic that is even partially distributed, the developer has to compose:

- containers and orchestrators,
- external databases,
- ad-hoc synchronization systems,
- very different execution environments (browser, server, edge, IoT),
- chains of dependencies, permissions, versions, configurations.

The result is almost always the same: a fragile ecosystem, hard to understand end-to-end, expensive to maintain and poorly portable.

### 1.2 Numax

Numax is a **portable runtime written in Rust** designed to run distributed applications in a simple, safe and consistent way across different environments.

The architecture is based on three and only three components:

1. **Execution of WebAssembly modules in an isolated sandbox** *(Implemented)*
   Wasmtime as the engine, WASI preview1 as the I/O baseline, minimal and controlled host API under the `nx` namespace.

2. **Local embedded key/value datastore** *(Implemented)*
   Based on `sled`. State lives next to the computation: low latency, no external dependencies, native offline operation.

3. **Distributed state synchronization via CRDT + gossip** *(Prototype)*
   Automatic replication between nodes, without centralized coordination, without distributed locks, with convergence guaranteed by the mathematical properties of CRDTs.

Numax is not a container, not an orchestrator, not a distributed database. It is a runtime: the minimum unit needed to execute distributed logic carrying state and synchronization with it.

### 1.3 The technological core

The principles that guide every technical choice:

- **Architectural simplicity as a guiding principle.** The runtime integrates only what is truly necessary: compute, local state, synchronization. Everything else stays optional or external.

- **State and code in the same environment.** The datastore is not a remote service: it is part of the runtime. Zero latency between computation and state, local ACID consistency, offline resilience as the default.

- **WASM as a portable unit of computation.** A single `.wasm` artifact can run on server, edge, browser, embedded devices, without conditional branching and without multiple codebases.

- **CRDT instead of locks or distributed transactions.** Synchronization does not require centralized coordination: CRDTs guarantee automatic convergence even with latency, partitions and concurrent updates.

- **Offline operation as a native feature.** Every node is self-sufficient. When it comes back online, it reconciles through CRDTs without conflicts and without additional application code.

Numax does not claim to eliminate the complexity of the distributed domain: it **incorporates it into the runtime in a systematic way**, and rejects the self-imposed complexity that today dominates distributed systems development.

---

## 2. Context

### 2.1 Necessary complexity vs self-imposed complexity

Building distributed systems involves an irreducible share of complexity. A good part of what we see today in our stacks, however, does not come from the problem: it comes from the tools.

**Necessary complexity** - intrinsic to the domain:

- the network is unreliable, introduces delays, disconnections, partitions;
- multiple nodes can update the same state in parallel;
- clients can go offline and come back at arbitrary moments;
- execution environments are heterogeneous (browser, server, mobile, IoT).

These aspects **cannot be avoided**: they require robust data models and synchronization mechanisms.

**Self-imposed complexity** - added by tools, not by the problem:

- complex orchestrators even for small applications;
- chains of dependencies between services and external infrastructure;
- configuration fragmented across dozens of files (YAML, custom operators, charts);
- state delegated to remote databases even when a local replica would be more efficient;
- different toolchains per environment (dev, browser, edge, IoT).

This complexity is **largely avoidable**: it comes from the accumulation of general-purpose technologies applied to contexts where they are not essential.

### 2.2 Opportunity

WebAssembly and CRDTs, taken together, make it possible to rethink the foundation on which we build distributed systems:

- truly portable execution across architectures and environments,
- sandbox isolation by default,
- state synchronization founded on provable mathematical properties,
- lightweight runtimes, independent of a specific infrastructure.

Numax is born in this space.

---

## 3. Numax design principles

### 3.1 The three main elements

Numax integrates three components, and only three:

1. execution of WASM modules in a sandbox *(Implemented)*
2. always-available local datastore *(Implemented)*
3. distributed state synchronization *(Prototype)*

Everything else belongs to upper layers or external tools. The runtime stays intentionally minimal: this is a choice, not a missing feature.

### 3.2 Radical portability

A WASM module must be able to run without modifications:

- on premise,
- in cloud,
- on edge nodes,
- on embedded devices,
- in the browser.

This reduces environment-specific configurations, platform dependencies and conditional branching in the application code to zero.

### 3.3 State close to computation

The runtime assumes that state must:

- be **local**, to guarantee speed and resilience;
- be **replicable**, to guarantee distribution *(Prototype)*.

The datastore is therefore integrated into the runtime and does not depend on external components. The computation does not travel to the state: the state is already there.

### 3.4 Conflict-free synchronization

Replication relies on CRDT models that allow:

- concurrent updates without locks;
- provable eventual consistency;
- absence of conflicts requiring manual resolution.

The network is considered fallible by nature. Disconnections, latency and reconnections are **normal** conditions, not exceptional ones.

---

## 4. Numax Overview

### 4.1 Main components

Numax is composed of six Rust crates organized in a workspace:

| Crate | Status | Responsibility |
|-------|--------|----------------|
| **nx-core** | *Implemented* | WASM runtime (Wasmtime), sandboxing, host API |
| **nx-store** | *Implemented* | Persistent local key/value datastore (sled) |
| **nx-sync** | *Implemented* | CRDT data structures, operations, node identity, SyncManager |
| **nx-net** | *Implemented* | TCP networking, message protocol, TLS 1.3 + mTLS |
| **nx-sdk** | *Implemented* | SDK to develop WASM guest modules |
| **nx-cli** | *Implemented* | Command line interface |

The separation keeps responsibilities clear and allows components to evolve independently.

### 4.2 Supported environments

Numax is designed to run on:

- servers (x86_64, ARM64),
- edge nodes,
- browsers (via WASM),
- mobile (via native integration),
- IoT (ARM / RISC-V).

CI today verifies compilation and test execution on:

- Ubuntu (x86_64),
- macOS (x86_64, ARM64),
- Windows (x86_64).

### 4.3 Execution and data model

- **Compute**: a Numax node runs WASM modules in a sandbox, exposing a limited set of host APIs. *(Implemented)*
- **State**: each node maintains a persistent local key/value store based on sled. *(Implemented)*
- **Sync**: a portion of the state can be replicated between nodes via CRDT + gossip. *(Prototype)*
- **Consistency**: the system aims for eventual consistency; in the absence of new writes and with sufficient connectivity, all nodes converge on the same state. *(Prototype)*
- **Fallible network**: disconnections and reconnections are normal conditions; automatic reconnect, peer health tracking, peer rotation and anti-entropy are implemented for configured peers. *(Prototype)*

### 4.4 Security model & threat model

**Assumptions:**

- the network is potentially hostile (observation, MITM, packet injection, route hijack);
- nodes can be offline or intermittent;
- some peers may be malicious or unreliable.

**Security goals:**

- Compute isolation (WASM sandbox). *(Implemented)*
- Confidentiality and integrity of communications between nodes. *(Implemented - TLS 1.3)*
- Mutual peer authentication. *(Implemented - mTLS, NodeID derived from the hash of the certificate's public key)*
- Membership controlled via a trusted peer allowlist. *(Implemented)*
- Full channel resilience across all scenarios (replay, downgrade, advanced certificate pinning, automatic rotation). *(Prototype / Planned)*

**Implemented guardrails:**

| Resource | Limit |
|----------|-------|
| Key length | 1024 bytes |
| Value length | 1 MB |
| Output buffer | 10 MB |

All input coming from the guest is validated before being processed.

**Out of scope (today):**

- logical bugs in the application module;
- data poisoning if untrusted peers are accepted without a policy;
- host-level compromise (the loss of a node's private key requires external revocation/rotation).

---

## 5. System Architecture

High-level overview of the components and their interactions.

```
┌─────────────────────────────────────────────────────────────┐
│                      WASM Module (Guest)                    │
│                    (compiled with nx-sdk)                   │
└──────────────────────────┬──────────────────────────────────┘
                           │ Host API calls (namespace "nx")
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                       nx-core (Host)                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Wasmtime   │  │  Host API   │  │    WASI (preview1)  │  │
│  │   Engine    │  │ db_*, log,  │  │    stdio, args      │  │
│  │             │  │ crdt_*      │  │                     │  │
│  └─────────────┘  └──────┬──────┘  └─────────────────────┘  │
└──────────────────────────┼──────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌────────────┐   ┌────────────┐   ┌────────────────────┐
   │  nx-store  │   │  nx-sync   │   │      nx-net        │
   │   (sled)   │◄──┤ SyncMgr +  ├──►│  TCP + TLS 1.3     │
   │            │   │   CRDTs    │   │  (mTLS, allowlist) │
   └────────────┘   └────────────┘   └────────────────────┘
                          ▲
                          │ async runtime (tokio)
                          ▼
                  Peer nodes (gossip, K-fanout)
```

The host runtime is entirely **async, based on tokio**: WASM module execution, network I/O, store operations and CRDT operation propagation are coordinated by the same asynchronous scheduler. The **SyncManager** is the junction point between CRDT host API, local store and network: it receives the operations generated by the guest, materializes them on sled and propagates them to the active peers.

### 5.1 Numax Core - WASM Runtime *(Implemented)*

**Main responsibilities:**

- load and run WASM modules,
- manage the sandbox with strict isolation,
- expose host functions to the guest module,
- integrate WASI preview1 as the standard I/O baseline.

**Technologies:**

- Rust implementation,
- **Wasmtime** as the WASM engine,
- WASI preview1 as the system interface,
- **tokio** asynchronous runtime for the host.

**Characteristics:**

- strict isolation: the guest cannot access resources not explicitly granted;
- no implicit filesystem access;
- fast startup (typically below ten milliseconds);
- memory-safety guaranteed by Rust and the WASM model.

**Return code convention:**

Host functions return integers with precise semantics:

| Code | Constant | Meaning |
|------|----------|---------|
| `>= 0` | - | Success (for `db_get`: length of the read value) |
| `0` | `OK` | Success (for `db_set`, `db_delete`, CRDT host API) |
| `-1` | `ERR_NOT_FOUND` | Key not found |
| `-2` | `ERR_BUFFER_TOO_SMALL` | Output buffer too small, retry with a larger buffer |
| `-3` | `ERR_INTERNAL` | Internal runtime error |
| `-4` | `ERR_RESERVED_KEY` | Attempt to use a key reserved by the runtime |
| `-5` | `ERR_SYNC_DISABLED` | Sync operation requested but sync is not enabled |

This convention allows the guest to handle errors deterministically, without exceptions or panic.

**Security limits (guardrails):**

| Resource | Limit |
|----------|-------|
| Key length | 1024 bytes |
| Value length | 1 MB |
| Output buffer | 10 MB |
| Peer connections | 64 |
| Queued CRDT ops | 10000 |
| Wire message size | 16 MiB |
| Socket read/write timeout | 30s |

### 5.2 Numax Store - Local datastore *(Implemented)*

Numax Store provides a persistent local key/value store for each runtime instance.

**Implementation:**

The datastore is based on **sled**, an embedded database written in Rust that offers:

- on-disk persistence,
- atomic operations,
- high performance for mixed read/write workloads,
- no external configuration required.

**API (Rust side):**

```rust
impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError>
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError>
    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), StoreError>
    pub fn delete(&self, key: &[u8]) -> Result<(), StoreError>
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StoreError>
}
```

**API (WASM guest side, via nx-sdk):**

```rust
use nx_sdk::db;

let value: Option<Vec<u8>> = db::get("my_key")?;
db::set("my_key", b"my_value")?;
db::delete("my_key")?;
```

The SDK automatically handles serialization, buffers and retries in case of `ERR_BUFFER_TOO_SMALL`.

**Properties:**

- local ACID for individual operations;
- atomic get/set/delete;
- no explicit locking required from the caller;
- data persistent across runtime restarts.

### 5.3 Numax Sync - Distributed replication *(Implemented core, Prototype end-to-end)*

Numax Sync is responsible for replicating state between nodes. The fundamental primitives are implemented and covered by tests (including the end-to-end wiring of the SyncManager). The full multi-process CLI cycle is tracked as Phase 7 of the roadmap.

**Components:**

**NodeId** *(Implemented)* - uniquely identifies a node. In TLS mode, the NodeId is **deterministically derived from the hash of the certificate's public key**: a node's identity is its key.

**Op and OpId** *(Implemented)* - CRDT operations that are serializable and transportable between nodes.

```rust
pub struct Op {
    pub id: OpId,           // Unique identifier of the operation
    pub origin: NodeId,     // Node that generated the operation
    pub timestamp: u64,     // Logical timestamp
    pub kind: OpKind,       // Operation type (e.g. GCounterIncrement)
}
```

Operations are serializable for transport over the network. The current wire protocol is length-prefixed and format-tagged: bincode is the default production format, while JSON remains available through `--debug-protocol`.

**GCounter (Grow-only Counter)** *(Implemented)* - the first implemented CRDT, a distributed counter that only supports increments. Each node owns its own "slot" and can only increment that one.

```rust
pub struct GCounter {
    counts: HashMap<String, u64>,  // NodeId -> local value
}
```

The total value is the sum of all slots: `value() = Σ counts[node]`.

**Guaranteed and verified CRDT properties:**

1. **Commutativity** - `merge(A, B) == merge(B, A)`
2. **Associativity** - `merge(merge(A, B), C) == merge(A, merge(B, C))`
3. **Idempotency** - `merge(A, A) == A`

Verified by dedicated tests in the `nx-sync` suite.

**Merge operation:**

```rust
pub fn merge(&mut self, other: &GCounter) {
    for (node, &value) in &other.counts {
        let entry = self.counts.entry(node.clone()).or_insert(0);
        *entry = (*entry).max(value); // Takes the maximum per slot
    }
}
```

**Overflow protection:** increments use `saturating_add` to saturate at `u64::MAX` instead of overflowing.

**SyncManager** *(Implemented)* - async component that integrates CRDT, store and network:

- receives operations from the guest via the CRDT host API,
- materializes the CRDT state on sled,
- propagates operations to active peers through nx-net,
- covered by end-to-end E2E tests.

**Hydration** *(Implemented)* - on startup, the runtime rebuilds the in-memory GCounter registry from durable CRDT state/op-log data, with materialized sled totals retained as a fallback. Dedup metadata is also persisted so recent duplicate remote operations after restart do not double count.

**Planned CRDTs (Phase 14):**

| Type | Description | Status |
|------|-------------|--------|
| PNCounter | Counter with increments and decrements | *Planned* |
| LWW-Register | Last-writer-wins register | *Planned* |
| ORSet | Set with observed add/remove | *Planned* |
| LWW-Map | Map with LWW semantics | *Planned* |
| RGA | Replicated Growable Array (sequences) | *Planned* |

### 5.4 Numax Net - Networking *(Implemented base, Prototype resilience)*

Numax Net handles communication between nodes for state synchronization.

**Architecture:** peer-to-peer. Each node can communicate directly with other nodes without a central server. TCP transport, **TLS 1.3 with mTLS** available and recommended.

**Message protocol:** length-prefixed, dual-mode serialization. The first four
bytes are a big-endian payload length. The payload starts with a one-byte
serialization format tag, followed by either JSON or bincode encoded message
data. Bincode is the default production format; JSON is selected with
`--debug-protocol` for inspectability.

```
┌──────────────┬──────────────┬─────────────────────────────┐
│ Length (4B)  │ Format (1B)  │ JSON or bincode payload      │
│ big-endian   │              │                             │
└──────────────┴──────────────┴─────────────────────────────┘
```

**Implemented message types:**

| Message | Direction | Description |
|---------|-----------|-------------|
| `Hello` | Client → Server | Initial handshake with NodeId, protocol version, supported formats and preferred format |
| `HelloAck` | Server → Client | Handshake confirmation with selected serialization format |
| `PushOps` | Bidirectional | Sends a batch of CRDT operations |
| `PushOpsAck` | Bidirectional | Confirms reception of operations |
| `PullSince` | Client → Server | Requests operations after a given OpId |
| `Ping` | Bidirectional | Keepalive |
| `Pong` | Bidirectional | Response to Ping |

**Protocol versioning:** version number (`PROTOCOL_VERSION = 2`) exchanged during the handshake. Version mismatches are rejected during handshake to avoid mixed-version wire ambiguity.

**Current status:**

- TCP + TLS 1.3 + mTLS channel *(Implemented)*;
- handshake, push/pull, anti-entropy pull requests and keepalive *(Implemented)*;
- peer connection limit, queued-op limit, message-size limit and socket read/write timeouts *(Implemented)*;
- automatic reconnect with exponential backoff, peer health tracking and peer rotation *(Prototype)*;
- periodic anti-entropy after missed pushes/reconnects *(Prototype)*;
- bounded OpId deduplication and persisted dedup metadata *(Prototype)*;
- peer-to-peer gossip with K-fanout: architecture defined, full dynamic discovery/fanout remains future work *(Prototype)*.

### 5.5 Channel security *(Implemented)*

Numax assumes a hostile network: the transport can be observed, altered or redirected. Communications between nodes happen by default over encrypted and authenticated channels.

**Guaranteed today:**

- **Confidentiality** - TLS 1.3 encrypts all traffic between nodes.
- **Integrity** - TLS 1.3 protects against payload tampering.
- **Mutual authentication** - mTLS: each node presents a certificate and verifies the peer's.
- **Verifiable identity** - the `NodeId` is derived from the hash of the certificate's public key. A node's identity cannot be forged without its private key.
- **Controlled membership** - explicit allowlist of accepted NodeIds/peers.
- **Forward Secrecy** - provided by TLS 1.3 cipher suites.

**Dedicated CLI flags:** `--tls-cert`, `--tls-key`, `--tls-ca`, `--allowed-peers`, `--tls-insecure` (the latter only for local development).

**Out of scope for v0.1.0-alpha.4:**

- automatic certificate rotation;
- advanced certificate pinning;
- complete channel hardening for all operational scenarios (object of work in subsequent phases).

### 5.6 Numax SDK *(Implemented)*

The SDK provides an ergonomic interface to develop WASM guest modules.

**Available modules:**

| Module | Functionality |
|--------|---------------|
| `nx_sdk::db` | Access to the datastore (get, set, delete) |
| `nx_sdk::log` | Structured logging toward the host |
| `nx_sdk::crdt::gcounter` | Ergonomic API to increment and read distributed GCounters |

**Example: distributed counter**

```rust
use nx_sdk::{log, crdt::gcounter};

#[no_mangle]
pub extern "C" fn run() {
    log::info("Module started");

    if let Err(e) = gcounter::inc("visits", 1) {
        log::error(&format!("inc error: {:?}", e));
        return;
    }

    match gcounter::value("visits") {
        Ok(v) => log::info(&format!("visits = {}", v)),
        Err(e) => log::error(&format!("read error: {:?}", e)),
    }
}
```

**Automatic buffer handling:** the SDK transparently handles the `ERR_BUFFER_TOO_SMALL` case by reallocating and retrying the call, so the developer never has to think about buffer sizes.

### 5.7 Numax CLI *(Implemented)*

The CLI is the main interface to run a Numax node.

```bash
# Runs a WASM module once
nx run <module.wasm>

# Runs with a custom data directory
nx run <module.wasm> --datastore-path ./my-data

# Runs with a TOML configuration file
nx run <module.wasm> --config ./numax.toml

# Runs with the observability endpoint enabled
nx run <module.wasm> \
    --observability-listen 127.0.0.1:9100 \
    --log-level info \
    --log-format json

# Runs as a sync-enabled node
nx run <module.wasm> \
    --listen 0.0.0.0:9000 \
    --peer 192.168.1.10:9000 \
    --peer 192.168.1.11:9000 \
    --datastore-path ./node-data

# Uses JSON instead of bincode for the sync wire protocol
nx run <module.wasm> \
    --listen 0.0.0.0:9000 \
    --peer 192.168.1.10:9000 \
    --debug-protocol

# Runs a bounded sync demo and prints a final GCounter value
nx run <module.wasm> \
    --listen 0.0.0.0:9000 \
    --peer 192.168.1.10:9000 \
    --wait-before-run 1500ms \
    --settle-for 2s \
    --print-gcounter counter:visits

# Adds mTLS and peer allowlisting to a sync node
nx run <module.wasm> \
    --listen 0.0.0.0:9000 \
    --tls-cert ./certs/node.crt \
    --tls-key ./certs/node.key \
    --tls-ca ./certs/ca.crt \
    --allowed-peers id1,id2,id3
```

**Main options:**

| Flag | Description |
|------|-------------|
| `--datastore-path` | Directory for persistent data |
| `--config` | TOML configuration file |
| `--observability-listen` | Enable `/metrics`, `/health` and `/ready` on the given address |
| `--log-level` | Logging level: `trace`, `debug`, `info`, `warn`, `error` |
| `--log-format` | Logging format: `text` or `json` |
| `--listen` | Address on which to accept peer connections and enable sync |
| `--peer` | Initial peer address (repeatable) |
| `--debug-protocol` | Use JSON for the sync wire protocol instead of production bincode |
| `--wait-before-run` | Bounded pre-run window for peer handshakes |
| `--settle-for` | Bounded post-run window for PushOps and remote apply |
| `--print-gcounter` | Print a final host-side GCounter value |
| `--shutdown-timeout` | Maximum graceful shutdown duration (default 30s) |
| `--tls-cert` / `--tls-key` / `--tls-ca` | TLS material for mTLS |
| `--allowed-peers` | Allowlist of accepted peer NodeIDs |
| `--tls-insecure` | Disables TLS (local dev only) |

The implemented `limits` section is intentionally small:

```toml
[limits]
max_peers = 64
queued_ops_limit = 10000
max_message_size = "16MiB"
socket_timeout_secs = 30

[observability]
listen = "127.0.0.1:9100"
log_level = "info"
log_format = "text"
request_timeout_secs = 5
```

The same limit values are the runtime defaults when no config file is provided.
The observability endpoint is opt-in: without `--observability-listen` or
`[observability].listen`, no HTTP endpoint is opened.

The endpoint exposes:

| Path | Meaning |
|------|---------|
| `/metrics` | Prometheus-compatible text metrics |
| `/health` | Liveness |
| `/ready` | Readiness |

The first metric set is deliberately operational:
operation counts, connected peers, last sync latency, sync errors,
observability request/error counters, peer connect/disconnect counters,
broadcast batch/op counters and local store size.

### 5.8 Topology: epidemic gossip *(Prototype)*

Numax does not assume a ring topology (e.g. `n1→n2→n3→…`): it would be fragile, the failure of one node would break the chain.

The model is **peer-to-peer with gossip**:

- each node maintains active connections to a subset of peers (fanout **K**);
- updates (CRDT operations) propagate in an "epidemic" fashion: a node sends the update to its peers, the peers forward it to others, until the network is covered;
- each operation has a unique identifier (`OpId`) to **deduplicate** and prevent loops.

The approach scales better than full-mesh and remains resilient in the presence of temporary disconnections. Full integration of dynamic fanout is in progress.

### 5.9 Resilience: node down, intermittent network, reconnection *(Prototype - Phase 10)*

The network is considered fallible by nature. The implemented countermeasures
for configured peers are:

When a peer becomes unreachable:

- timeout and retry with **exponential backoff**;
- peer health tracking with suspect/dead state after repeated failures;
- peer rotation across configured peers when slots are available.

When a node comes back:

- re-establishment of connections with known peers;
- **anti-entropy** mechanism (`PullSince`) to recover missing updates;
- durable CRDT state/op-log hydration on restart;
- persisted bounded dedup metadata to avoid recent duplicate remote operations
  after restart;
- convergence to the same state thanks to CRDT properties.

---

## 6. Programming Model

### 6.1 WASM modules as units of computation *(Implemented)*

A Numax application is composed of one or more WASM modules that:

- execute pure application logic,
- read/write to the local datastore via host API,
- publish and receive CRDT updates via dedicated host API,
- (future) will make HTTP calls if explicitly permitted.

The module must expose a `run` function with the signature:

```rust
#[no_mangle]
pub extern "C" fn run() {
    // application logic
}
```

### 6.2 Host API exposed to modules

**Import namespace:** `"nx"`. All host functions are imported from the `"nx"` namespace; the SDK provides type-safe wrappers.

**Database** - *(Implemented)*

| Function | Signature | Status |
|----------|-----------|--------|
| `db_get` | `(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32` | *Implemented* |
| `db_set` | `(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32` | *Implemented* |
| `db_delete` | `(key_ptr: u32, key_len: u32) -> i32` | *Implemented* |
| `db_exists` | `(key_ptr: u32, key_len: u32) -> i32` | *Implemented (Phase 12)* |
| `db_scan` | `(prefix_ptr: u32, prefix_len: u32, cursor: u64, limit: u32, out_ptr: u32, out_cap: u32) -> i32` | *Implemented (Phase 12)* |
| `db_keys` | `(prefix_ptr: u32, prefix_len: u32, cursor: u64, limit: u32, out_ptr: u32, out_cap: u32) -> i32` | *Implemented (Phase 12)* |

**Logging** - *(Implemented)*

| Function | Signature | Status |
|----------|-----------|--------|
| `host_log_v2` | `(level: u32, msg_ptr: u32, msg_len: u32) -> ()` | *Implemented* |

Log levels: 0 = trace, 1 = debug, 2 = info, 3 = warn, 4 = error.

**CRDT** - *(Implemented)*

| Function | Signature | Status |
|----------|-----------|--------|
| `crdt_gcounter_inc` | `(key_ptr: u32, key_len: u32, delta: u64) -> i32` | *Implemented* |
| `crdt_gcounter_value` | `(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32` | *Implemented* |

Increment operations are persisted as CRDT state/op-log metadata, materialized on sled and propagated to peers through the SyncManager. In the absence of sync enabled, the functions return `ERR_SYNC_DISABLED` (-5).

**Extended Host API** - *(Planned, Phase 12)*

| Function | Description | Status |
|----------|-------------|--------|
| `time_now` | Current Unix timestamp in milliseconds | *Implemented (Phase 12)* |
| `time_monotonic` | Monotonic milliseconds for elapsed-time measurements | *Implemented (Phase 12)* |
| `random_bytes` | Cryptographically secure random bytes | *Implemented (Phase 12)* |
| `hash_sha256` | SHA-256 digest | *Implemented (Phase 12)* |
| `hash_blake3` | BLAKE3 digest | *Implemented (Phase 12)* |
| `env_get` | Filtered environment variable reading | *Planned* |
| `http_fetch` | HTTP request with whitelist | *Planned* |

### 6.3 Configuration and Deployment *(Prototype)*

Deployment will consist in shipping a `.wasm` file and a minimal configuration.

The `limits` section below is implemented today. Observability can also be
configured from file. The other deployment sections remain planned work.

```toml
[limits]
max_peers = 64
queued_ops_limit = 10000
max_message_size = "16MiB"
socket_timeout_secs = 30

[module]
name = "cart_handler"
path = "cart_handler.wasm"

[permissions]
db = true
network = ["https://api.example.com"]

[sync]
enabled = true
keys = ["cart:", "user:"]
```

---

## 7. Test Suite

The project includes an automated test suite that covers runtime, store, CRDT, networking and end-to-end flows.

**Current coverage:** more than **38 tests** across unit, integration and end-to-end, distributed across the workspace crates (nx-core, nx-store, nx-sync, nx-net) and the SyncManager E2E flows.

**Specific CRDT tests** - explicitly verify the mathematical properties:

- `test_gcounter_merge_commutativity` - `merge(A, B) == merge(B, A)`
- `test_gcounter_merge_associativity` - `(A⊕B)⊕C == A⊕(B⊕C)`
- `test_gcounter_merge_idempotency` - `A⊕A == A`
- `test_gcounter_overflow_protection` - saturation instead of overflow

**SyncManager E2E tests** - verify that an operation generated by the WASM guest is materialized on sled and correctly propagated to peers.

**CI/CD:** GitHub Actions runs the pipeline on every push/PR on Ubuntu, macOS, Windows. Main jobs:

1. `check` - compilation check
2. `fmt` - code formatting check
3. `clippy` - Rust linter
4. `test` - full test suite execution
5. `build-wasm` - compilation of WASM examples

**Load testing** - *Planned, Phase 13*.

---

## 8. Use Cases

The use cases below are **concretely achievable today** with the primitives of v0.1.0-alpha.4. They do not describe visions: they describe what the runtime already knows how to do, or will know how to do as soon as the last preview phases are closed.

### 8.1 Distributed counters and metrics (example: `distributed_counter`)

**Problem.** You need global counters - visits, events, likes, throttle counters - on geographically distributed nodes, without a single point of centralization.

**Why Numax.** A GCounter CRDT guarantees that each node can increment its own slot locally, without coordination, and that totals converge automatically. No central database, no distributed locks.

**What you need today.** A WASM module that calls `crdt_gcounter_inc` / `crdt_gcounter_value` via the SDK; multiple `nx run` instances with `--listen` / `--peer`, optionally protected by mTLS. This is exactly what the `distributed_counter` example in the repo does.

### 8.2 Voting, polling, distributed tally (example: `vote_tally_tls`)

**Problem.** Aggregating counts (votes, reports, choices) coming from independent nodes - typically in environments where trust between nodes must be verified and the channel cannot be considered secure.

**Why Numax.** GCounter for the counts + mTLS with allowlist to guarantee that **only authorized peers** can contribute. The peer's identity is its key (NodeID = hash of the public key): an unauthorized peer cannot inject votes by impersonating another.

**What you need today.** A WASM module that increments the counter corresponding to the chosen vote + `nx run` nodes with TLS, certificates for each node and `--allowed-peers`. This is exactly the scenario of the `vote_tally_tls` example.

### 8.3 Edge computing with local state and reconciliation

**Problem.** On edge nodes (industrial gateways, physical stores, vehicles, remote infrastructure) you want to run application logic **close to the data**, maintaining state even if the connection toward the "center" is intermittent.

**Why Numax.** State lives in the sled store of the edge node, always available locally. Operations are replicated to peers (other edge nodes or backhaul nodes) as soon as the network is available. CRDT properties guarantee that, once reconnected, the node loses or duplicates nothing.

The compute is portable: the same `.wasm` module runs on an ARM gateway, on a cloud server for central rollup and - prospectively - on the browser for local dashboards.

### 8.4 Offline-first and collaborative applications

**Problem.** Applications that must work without a connection (collaborative notes, distributed configurations, field applications, maritime/aerial/rural devices) and reconcile when they come back online, without imposing manual conflict resolution.

**Why Numax.** This is exactly the sweet spot of CRDTs: each node operates locally on its own store, changes propagate opportunistically, convergence is mathematically guaranteed. When PNCounter, LWW-Register, ORSet, LWW-Map and RGA arrive (Phase 14), the model will cover most real offline-first patterns.

The `distributed_chat` example (today in local-only mode) represents the skeleton of this use case.

---

## 9. Where Numax stands

Numax is easy to describe by **difference**.

- **It is not Kubernetes.** It does not orchestrate containers, it does not manage workloads on clusters, there is no control plane. Numax is a single binary that runs WASM modules. If Kubernetes answers "how do I orchestrate hundreds of services?", Numax answers a prior question: "why do I need hundreds of services to do one distributed thing?".

- **It is not Redis (nor a distributed database).** It is not a remote service you query. State does not live "somewhere on the network": it lives **inside the runtime**, next to the code that uses it. Replication happens between peer runtimes, not between client and server. (ps: I love redis)

- **It is not Deno or another "edge JavaScript" runtime.** Those runtimes bring a language everywhere. Numax brings **logic + state + synchronization** everywhere, in a single coherent model. The language is a detail: the unit is the WASM module, agnostic with respect to its source.

- **It is not a CRDT framework.** Yjs, Automerge and similar are excellent libraries, but they are libraries: they leave the developer to design transport, persistence, identity, sandbox. Numax incorporates all of this into a runtime and provides CRDT structures as **first-class primitives** accessible to the guest via host API.

**What Numax is, then.** A **minimal portable runtime** that combines, in a single process, the three elements needed to build distributed applications: isolated compute (WASM), local state (sled), coordination-free synchronization (CRDT + gossip).

The idea is simple and radical: **run distributed applications without building a distributed infrastructure**.

### A note on the AI era

Numax itself was largely written using artificial intelligence. It would be hypocritical to pretend otherwise, and that is not the point: AI today is a working tool, exactly as a compiler, a debugger or an editor are. The interesting question is not whether AI is used to build software, but what is being built.

And here Numax does something different from much of the software that is born in this "season". It does not generate, does not predict, does not classify. It is not yet another interface on top of a model. It solves a structural problem, the one of those who build distributed software, that exists regardless of AI and that, if anything, AI makes more urgent: today's systems must coordinate models, data and compute across edge, cloud and devices, in a reliable and portable way.

Numax is not AI. It is one of the things that AI can, comfortably, run on top of.

**Numax is a runtime for those who want to build distributed systems without building a distributed infrastructure.**

---

## 10. Limitations

v0.1.0-alpha.4 is a technical preview. We recognize its limits, explicitly:

- **Network resilience is still prototype-grade.** Automatic reconnect, peer health tracking, peer rotation, anti-entropy and bounded dedup are implemented for configured peers, but full dynamic discovery and K-fanout gossip remain future work.
- **Deduplication is bounded.** Recent duplicate remote operations are prevented across restart, but this is not an infinite causal history. Stronger guarantees would require a fuller durable op-log/causal metadata strategy.
- **TLS/mTLS is implemented, but not yet hardened for all scenarios.** It is solid enough for controlled scenarios (dev, lab, defined deployments); the full hardening (rotation, advanced pinning, extreme hostile scenarios) continues.
- **Minimal observability.** Structured logs, Prometheus-compatible metrics and health checks are available as an opt-in endpoint; richer dashboards and alerting remain outside the current preview.
- **Wire format and Host API can change.** Before the stable v0.1.0, we expect non-backward-compatible changes. The current wire protocol is versioned (`PROTOCOL_VERSION = 2`) and supports bincode by default with JSON debug mode.
- **Available CRDTs limited to GCounter.** PNCounter, LWW-Register, ORSet, LWW-Map and RGA will arrive with Phase 14.
- **It does not replace complex orchestrators.** It is not designed to manage extensive clusters or highly scalable deployments with advanced scheduling.
- **Not optimized for CPU-bound workloads.** The focus is I/O and coordination, not intensive computation.
- **Data models must be compatible with CRDTs.** Patterns based on locks or strong distributed transactions do not map directly.

These limits are not hidden weaknesses: they are the **honest perimeter** of a preview that aims to show the trajectory, not to sell a finished product.

---

## 11. Conclusions

Numax proposes a unified runtime that combines:

- safe and portable execution via WebAssembly,
- integrated local datastore for state close to computation,
- distributed synchronization based on CRDT and gossip,
- node identity and encrypted channel founded on mTLS.

The goal is not to replicate the existing ecosystem, but **to reduce the self-imposed complexity** that today dominates distributed systems development, while preserving control over the necessary complexity of one's own domain.

v0.1.0-alpha.4 is a technical preview. What it contains is real, tested, working: WASM runtime, sled store, GCounter CRDT, async SyncManager, TCP networking, TLS 1.3 + mTLS with identity derived from the key, stable host API for database, log and CRDT, lifecycle/backpressure hardening, network resilience for configured peers, dual-mode JSON/bincode serialization, opt-in observability, multi-OS CI, end-to-end examples.

What is still missing is declared explicitly and tracked in the roadmap. Subsequent iterations will refine details, practical examples, comparisons and experimental results.

**v0.1.0-alpha.4 is just the beginning.** But it is a beginning built on code, not on promises.

In closing, I love software and I love numax.

---

## Appendix A - Repository structure

```
numax/
├── Cargo.toml              # Workspace manifest
├── crates/
│   ├── nx-core/            # WASM Runtime + Host API
│   ├── nx-store/           # Local datastore (sled)
│   ├── nx-sync/            # CRDT, operations, SyncManager
│   ├── nx-net/             # Networking, protocol, TLS/mTLS
│   ├── nx-sdk/             # SDK for WASM guests
│   └── nx-cli/             # CLI
├── examples/
│   ├── distributed_counter/
│   ├── distributed_chat/
│   └── vote_tally_tls/
├── HOST_API.md
├── WHITEPAPER.md
├── ROADMAP_v0.1.0.md
└── LICENSE
```

---

## Appendix B - References

- WebAssembly: https://webassembly.org/
- WASI: https://wasi.dev/
- Wasmtime: https://wasmtime.dev/
- sled: https://sled.rs/
- CRDT: Shapiro et al., *A comprehensive study of Convergent and Commutative Replicated Data Types*
