---
title: nx-core
description: Runtime, host APIs and sync orchestration.
---

`nx-core` is the center of the Numax stack. It owns WASM execution, the full host API surface,
the sync manager, and observability. `nx-cli` builds a `RuntimeConfig` and hands it to this crate.
Everything below that boundary lives here or in the crates it composes.

---

## Responsibilities

| Responsibility | Where |
|---|---|
| WASM module loading, compilation, caching | `runtime.rs` - `Runtime::run_module`, `compile_or_get_cached_module` |
| Host API surface (all `nx` namespace imports) | `host_api/` - one file per API group |
| Lifecycle: start sync, run module, settle, serve, shutdown | `runtime.rs` - `Runtime` |
| Sync orchestration, in-memory CRDT registry and op-log | `sync_manager/manager.rs` - `SyncManager` |
| Remote operation application | `sync_manager/apply.rs` |
| Durable CRDT state and startup hydration | `sync_manager/storage.rs` |
| Anti-entropy, peer broadcast and reconnect handling | `sync_manager/replication.rs` + `nx-net` |
| Schema headers and offline migration support | `sync_manager/schema.rs`, `sync_manager/migration.rs` |
| Peer health tracking | `sync_manager/peer.rs` |
| NodeId persistence | `runtime.rs` - `load_or_create_node_id` |
| Observability HTTP endpoint | `observability.rs` - `ObservabilityServer` |
| Config types exposed to `nx-cli` | `sync_config.rs` - `SyncConfig`, re-exports `TlsConfig`, `ObservabilityConfig` |

---

## Runtime

The `Runtime` struct is the public API of `nx-core`. `nx-cli` builds one, calls methods on it
in order, then shuts it down.

```rust
pub struct Runtime {
    engine:               Engine,          // wasmtime engine, shared across runs
    linker:               Linker<HostState>, // all host API functions registered here
    config:               RuntimeConfig,
    store:                Arc<NxStore>,    // shared with every HostState and the sync manager
    metrics:              Arc<RuntimeMetrics>,
    module_cache:         Mutex<HashMap<[u8; 32], Module>>, // blake3 keyed
    sync_manager:         Option<SyncManager>,
    sync_handle:          Option<SyncHandle>, // cheap clone, passed to every HostState
    observability_server: Option<ObservabilityServer>,
}
```

### RuntimeConfig

```rust
pub struct RuntimeConfig {
    pub enable_wasi:       bool,           // default: true
    pub max_memory_bytes:  Option<u64>,    // per-invocation memory cap via StoreLimits
    pub datastore_path:    PathBuf,        // default: ./nx-data
    pub sync:              Option<SyncConfig>,
    pub observability:     Option<ObservabilityConfig>,
    pub module_id:         String,         // exposed to guest via system::module_id()
}
```

`sync: None` means no networking, no CRDT replication, no `SyncManager`.
The runtime still works - the store is local-only.

### HostState

One `HostState` is created per `run_module` invocation and attached to the wasmtime `Store`.

```rust
pub struct HostState {
    pub wasi:        Option<p1::WasiP1Ctx>,  // None when enable_wasi = false
    pub store:       Arc<NxStore>,           // same Arc as the Runtime
    pub sync_handle: Option<SyncHandle>,     // None when sync disabled
    pub module_id:   Arc<str>,
    pub limits:      wasmtime::StoreLimits,  // memory cap enforcement
}
```

### Lifecycle methods

Standard call order from `nx-cli`:

```
Runtime::new(config)
  └── start_observability()   optional, starts HTTP endpoint
  └── start_sync()            optional, starts SyncManager + networking
  └── wait_before_run(dur)    optional, waits for peers before running
  └── run_module(bytes)       loads, links, instantiates, calls run()
  └── settle_for(dur)         optional, keeps sync alive for a bounded window
      OR serve()              optional, keeps sync alive until SIGINT/SIGTERM/SIGHUP
  └── shutdown_with_timeout(dur)
```

| Method | What it does |
|---|---|
| `new(config)` | Opens sled store, builds wasmtime engine + linker with all host API functions registered, creates `SyncManager` if configured |
| `start_observability()` | Binds the HTTP metrics endpoint. No-op if not configured |
| `start_sync()` | Calls `SyncManager::start()`, starts TCP listener + dial loop. No-op if sync disabled |
| `wait_before_run(dur)` | Repeatedly reconnects configured peers until the deadline. No-op if sync disabled |
| `run_module(bytes)` | Compiles or retrieves cached module, builds `HostState`, instantiates, calls `run()` or `_start()` |
| `settle_for(dur)` | Sleeps for `dur`, keeping sync alive. No-op if sync disabled |
| `serve()` | Blocks until OS signal (SIGINT/SIGTERM/SIGHUP on Unix, Ctrl+C on Windows). No-op if sync disabled |
| `shutdown_with_timeout(dur)` | Stops sync manager, flushes sled store, shuts down observability server. Bounded by `dur` (default 30s) |

### Module compilation cache

Modules are compiled once and cached in a `Mutex<HashMap<[u8; 32], Module>>`, keyed by
the blake3 hash of the raw bytes. Repeated calls to `run_module` with the same bytes skip
compilation entirely. The cache lives for the lifetime of the `Runtime`.

### NodeId persistence

On first start with sync enabled, `load_or_create_node_id` generates a `NodeId` and stores
it under `__nx/runtime/node_id` in sled. On subsequent starts it reads the same key.
This ensures a node always presents the same identity to its peers across restarts.

---

## SyncConfig

`SyncConfig` is the builder passed inside `RuntimeConfig` when sync is needed.
`nx-cli` builds it in `config.rs`; `nx-core` consumes it in `SyncManager::new`.

```rust
SyncConfig::new()
    .with_listen_addr("0.0.0.0:9000")
    .with_peer("127.0.0.1:9001")
    .with_tls(TlsConfig::new(cert, key, ca))
    .with_max_peers(16)
    .with_queued_ops_limit(5000)
    .with_op_log_limit(5000)
    .with_seen_ops_limit(50000)
    .with_max_message_size(8 * 1024 * 1024)
    .with_socket_timeout(Duration::from_secs(15))
    .with_reconnect_backoff(Duration::from_millis(250), Duration::from_secs(15))
    .with_peer_dead_after_failures(5)
    .with_anti_entropy_interval(Duration::from_secs(60))
    .with_serialization_format(SerializationFormat::Bincode)
```

**`is_enabled()`** returns `true` only when `listen_addr` is set.
Peers alone do not enable sync - a node must also listen.

### Defaults

| Field | Default |
|---|---|
| `max_peers` | 64 |
| `queued_ops_limit` | 10 000 |
| `op_log_limit` | 10 000 |
| `seen_ops_limit` | 100 000 |
| `max_message_size` | 16 MiB |
| `socket_timeout` | 30s |
| `reconnect_initial_delay` | 500ms |
| `reconnect_max_delay` | 30s |
| `peer_dead_after_failures` | 3 |
| `anti_entropy_interval` | 30s |
| `serialization_format` | `Bincode` |

---

## SyncManager

`SyncManager` owns the runtime side of replication. It is the bridge between host API calls
from guest modules and the network layer in `nx-net`.

Since `v0.1.1`, its implementation is split by responsibility under `sync_manager/`:
orchestration in `manager.rs`, remote application in `apply.rs`, replication in
`replication.rs`, persistence in `storage.rs`, peer health in `peer.rs`, and persisted
schema evolution in `schema.rs` and `migration.rs`.

**What it owns:**
- In-memory CRDT registry (one state per CRDT key, all types)
- Op-log (bounded by `op_log_limit`) for anti-entropy replay
- Seen-ops set (bounded by `seen_ops_limit`) for deduplication
- Anti-entropy scheduling loop
- Peer broadcast queue (bounded by `queued_ops_limit`)

**What it does not own:**
- TCP connections and TLS (delegated to `nx-net::SyncNode`)
- CRDT data structures and merge logic (delegated to `nx-sync`)
- Sled store (shared `Arc<NxStore>` from the `Runtime`)

### SyncHandle

`SyncHandle` is a cheap clone of a channel endpoint into the `SyncManager`.
It is what the host API functions hold - they push ops into the manager via the handle
without blocking the guest.

```rust
// Inside a host API function (e.g. crdt.rs):
state.sync_handle.as_ref()
    .ok_or(ERR_SYNC_DISABLED)?
    .push_op(op)
    .await?;
```

### CRDT read-back methods

After `settle_for` or `serve`, `nx-cli` can read CRDT state via `Runtime`:

```rust
runtime.get_counter_value("counter:visits").await    // Option<u64>
runtime.get_pncounter_value("inventory:sku").await   // Option<i64>
runtime.get_lww_register_value("status:svc").await   // Option<Option<Vec<u8>>>
runtime.get_orset_elements("tags:item").await        // Option<Vec<String>>
runtime.get_lww_map_entries("settings:svc").await    // Option<Vec<(String, Vec<u8>)>>
runtime.get_rga_values("comments:doc").await         // Option<Vec<Vec<u8>>>
```

All return `None` when sync is disabled (used by `--print-*` flags in `nx-cli`).

---

## Host API

All host functions are registered in `Runtime::new` via `add_to_linker` calls:

```rust
host_api::log::add_to_linker(&mut linker)?;
host_api::db::add_to_linker(&mut linker)?;
host_api::time::add_to_linker(&mut linker)?;
host_api::crypto::add_to_linker(&mut linker)?;
host_api::system::add_to_linker(&mut linker)?;
host_api::net::add_to_linker(&mut linker)?;
host_api::crdt::add_to_linker(&mut linker)?;
```

Each file owns one group. They all follow the same pattern: read from guest linear memory,
do the work, write back to the output buffer, return byte count or error code.

| File | Functions registered |
|---|---|
| `host_api/log.rs` | `host_log`, `host_log_v2` |
| `host_api/db.rs` | `db_get`, `db_set`, `db_delete`, `db_exists`, `db_scan`, `db_scan_after`, `db_keys`, `db_keys_after` |
| `host_api/time.rs` | `time_now`, `time_monotonic` |
| `host_api/crypto.rs` | `random_bytes`, `hash_sha256`, `hash_blake3` |
| `host_api/system.rs` | `env_get`, `module_id`, `host_capabilities`, `event_emit`, `abort` |
| `host_api/net.rs` | `net_node_id`, `net_peers` |
| `host_api/crdt.rs` | all 18 CRDT functions |

For the full function signatures and behavior see [Host API](/numax/reference/host-api/).

### How to add a new host function (developer guide)

1. Add the raw FFI import to `nx-sdk/src/ffi.rs`.
2. Add the safe SDK wrapper in the appropriate `nx-sdk/src/*.rs` file.
3. Add the host implementation in the appropriate `nx-core/src/host_api/*.rs` file,
   following the read-from-guest-memory / write-to-output-buffer pattern.
4. Register it with `linker.func_wrap("nx", "function_name", ...)` inside `add_to_linker`.
5. Call `add_to_linker` from `Runtime::new`.
6. If it requires sync, check `state.sync_handle.is_some()` and return `ERR_SYNC_DISABLED` if not.
7. Write tests. For CRDT functions, add an E2E test under `sync_manager/tests/`.

---

## Observability

`ObservabilityServer` exposes a local HTTP endpoint when `RuntimeConfig.observability` is set.

`RuntimeMetrics` is an `Arc`-shared struct updated by the runtime and the sync manager.
It tracks readiness and basic counters accessible through the HTTP endpoint.

`metrics.set_ready(true)` is called after sync starts. `set_ready(false)` is called at shutdown start.

---

## Test coverage

Tests live in `runtime.rs` (`#[cfg(test)]` at the bottom), `sync_config.rs`, and
`sync_manager/tests/`.

| Test | What it covers |
|---|---|
| `serve_returns_immediately_when_sync_is_disabled` | `serve` is a no-op without sync |
| `serve_keeps_runtime_alive_until_shutdown` | `serve` blocks until signal, returns correct `ShutdownSignal` |
| `serve_returns_none_when_sync_is_disabled` | `serve_until_shutdown` returns `None` signal when sync off |
| `settle_returns_immediately_when_sync_is_disabled` | `settle_for` is a no-op without sync |
| `settle_waits_for_requested_duration_when_sync_is_enabled` | `settle_for` actually sleeps |
| `shutdown_with_timeout_flushes_store_without_sync` | store is flushed on shutdown |
| `run_module_reuses_compiled_module_for_same_bytes` | module cache works, count stays at 1 |
| `sync_runtime_reuses_persisted_node_id` | NodeId survives `Runtime` drop and re-open |
| `SyncConfig` tests | `is_enabled` requires listen addr, peers alone don't enable, all builder fields |

```bash
cargo test -p nx-core
```

---

## Related

Use this page together with the crates that feed into or are orchestrated by the runtime:

- [Host API](/numax/reference/host-api/) - full function reference for `host_api/`
- [nx-cli crate](/numax/reference/crates/nx-cli/) - builds `RuntimeConfig` and calls this crate
- [nx-sync crate](/numax/reference/crates/nx-sync/) - CRDT types consumed by the sync manager
- [nx-net crate](/numax/reference/crates/nx-net/) - networking delegated by the sync manager
- [Crates overview](/numax/reference/crates/) - full dependency graph
