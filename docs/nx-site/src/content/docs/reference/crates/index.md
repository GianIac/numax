---
title: Crates
description: Overview of the Numax Rust crates.
---

Numax is a Cargo workspace with six crates. Each one owns a single layer of the stack.
No crate reaches across its boundary.

```
nx-cli
  └── nx-core
        ├── nx-store
        ├── nx-sync
        └── nx-net
              └── nx-sync

nx-sdk          (standalone — targets wasm32, no internal deps)
```

---

## nx-cli

**What it owns:** the `nx` binary. Configuration parsing, CLI flag validation, precedence resolution, logging setup.

**Does not own:** runtime logic, WASM execution, networking. Everything below the CLI surface is delegated to `nx-core`.

**Produces:** the `nx` executable.

**Key files:**
- `src/main.rs` - command definitions (`nx run`, `nx config`, `nx migrate`), flag parsing via clap, runtime wiring
- `src/config.rs` - TOML file structs, environment variable resolution, effective config builder, `nx config init` template

**External dependencies:** `clap`, `tokio`, `tracing`, `tracing-subscriber`, `toml`, `serde`

---

## nx-core

**What it owns:** the runtime. WASM module loading and execution via Wasmtime, the full host API surface, the sync manager, observability.

**Does not own:** CRDT data structures (nx-sync), raw TCP/TLS (nx-net), the sled store (nx-store). It composes them.

**Key files:**
- `src/runtime.rs` - `Runtime` struct: starts/stops sync, runs the WASM module, exposes `settle_for`, `serve`, `shutdown_with_timeout`
- `src/sync_manager.rs` - owns the in-memory CRDT registry, op-log, anti-entropy scheduling, peer broadcast, durable state hydration on startup
- `src/sync_config.rs` - `SyncConfig` builder, `TlsConfig`, `ObservabilityConfig`
- `src/observability.rs` - HTTP metrics endpoint
- `src/host_api/db.rs` - `db_get`, `db_set`, `db_delete`, `db_exists`, `db_scan`, `db_scan_after`, `db_keys`, `db_keys_after`
- `src/host_api/crdt.rs` - all CRDT host functions: gcounter, pncounter, lww_register, lww_map, orset, rga
- `src/host_api/crypto.rs` - `random_bytes`, `hash_sha256`, `hash_blake3`
- `src/host_api/log.rs` - `host_log`, `host_log_v2`
- `src/host_api/net.rs` - `net_node_id`, `net_peers`
- `src/host_api/system.rs` - `env_get`, `module_id`, `host_capabilities`, `event_emit`, `abort`
- `src/host_api/time.rs` - `time_now`, `time_monotonic`

**External dependencies:** `wasmtime`, `wasmtime-wasi`, `tokio`, `tracing`, `blake3`, `sha2`, `getrandom`, `serde_json`

---

## nx-store

**What it owns:** the local embedded key/value store. A thin, typed wrapper over sled.

**Does not own:** CRDT logic, networking, anything sync-related.

**Key files:**
- `src/store.rs` - `Store`: `get`, `set`, `delete`, `exists`, prefix scan with offset and key cursors
- `src/lib.rs` - public API re-exports
- `src/error.rs` - `StoreError`

**External dependencies:** `sled`, `thiserror`

**Benches:** `single_node_load` - throughput benchmark for local read/write operations.

---

## nx-sync

**What it owns:** CRDT data structures and operation types. Pure logic — no I/O, no async, no networking.

This is the one crate that can be reasoned about and tested without any runtime. It defines what an operation is, how CRDTs merge, and what the serialized wire format looks like.

**Key files:**
- `src/op.rs` - `OpId`, `OpKind` (one variant per CRDT operation), `Op`, serialization for all op types
- `src/node_id.rs` - `NodeId` type and random generation
- `src/crdt/gcounter.rs` - GCounter state and merge
- `src/crdt/pncounter.rs` - PNCounter state and merge
- `src/crdt/lww_register.rs` - LWW-Register state and merge
- `src/crdt/lww_map.rs` - LWW-Map state and merge
- `src/crdt/orset.rs` - ORSet state and merge
- `src/crdt/rga.rs` - RGA state and merge

**External dependencies:** `serde`, `serde_json`, `uuid`, `thiserror`

---

## nx-net

**What it owns:** TCP networking, TLS/mTLS, message framing, gossip, peer management, anti-entropy loops.

**Does not own:** CRDT logic (consumed from nx-sync), store access (goes through nx-core's sync manager).

**Key files:**
- `src/node.rs` - `SyncNode`: listens for incoming connections, dials peers, manages reconnect backoff, runs broadcast and anti-entropy loops
- `src/message.rs` - `Message`, `MessageKind`, and `WireError` wire types: `Hello`, `HelloAck`, `PushOps`, `PushOpsAck`, `PullSince`, `Ping`, `Pong`, `Error`
- `src/tls.rs` - TLS acceptor/connector setup, certificate parsing, mTLS peer identity extraction, allowlist enforcement
- `src/peer.rs` - `PeerInfo`: address, NodeId, connection state
- `src/error.rs` - `NetError`

**External dependencies:** `tokio`, `tokio-rustls`, `rustls`, `rustls-pemfile`, `rcgen`, `bincode`, `serde`, `serde_json`, `sha2`, `hex`, `x509-parser`, `tracing`

**Benches:** `serialization` - wire format encode/decode throughput.

---

## nx-sdk

**What it owns:** the guest-side SDK for WASM modules. Runs inside the `.wasm` binary, not inside the Numax runtime.

Compiles to `wasm32-unknown-unknown`. Has no internal workspace dependencies and no runtime/async dependencies. The only thing it touches is the host via FFI imports.

**Key files:**
- `src/ffi.rs` - raw `unsafe extern "C"` imports from the `nx` namespace: every host function the SDK calls
- `src/db.rs` - safe `get`, `set`, `delete`, `exists`, `scan`, `keys` wrappers
- `src/log.rs` - `log(msg)` function and `nx_log!` macro
- `src/crypto.rs` - `random_bytes`, `hash_sha256`, `hash_blake3`
- `src/time.rs` - `now`, `monotonic`
- `src/net.rs` - `node_id`, `peers`
- `src/system.rs` - `env_get`, `module_id`
- `src/error.rs` - `NxError`, `Result`
- `src/crdt/gcounter.rs` - `inc`, `value`
- `src/crdt/pncounter.rs` - `inc`, `dec`, `value`
- `src/crdt/lww_register.rs` - `set`, `get`
- `src/crdt/lww_map.rs` - `set`, `remove`, `get`, `contains`, `entries`
- `src/crdt/orset.rs` - `add`, `remove`, `contains`, `elements`
- `src/crdt/rga.rs` - `insert_after`, `delete`, `values`

**External dependencies:** none. The `std` feature is optional and disabled by default.

---

## Dependency graph in full

```
nx-cli ──────────────────────────────────── bin: nx
  │
  └── nx-core ──────────────────────────── runtime, host API, sync manager
        │
        ├── nx-store ─────────────────────  sled KV store
        │
        ├── nx-sync ──────────────────────  CRDT types, op types, pure logic
        │
        └── nx-net ───────────────────────  TCP, TLS, gossip, anti-entropy
              │
              └── nx-sync

nx-sdk ───────────────────────────────────  guest SDK (wasm32, no internal deps)
```

---

## Where to go next

- [Host API](/numax/reference/host-api/) - the functions `nx-sdk` calls and `nx-core` implements
- [Configuration](/numax/reference/configuration/) - how `nx-cli` resolves config before passing it to `nx-core`
- [CLI](/numax/reference/cli/) - the user-facing `nx` command surface
