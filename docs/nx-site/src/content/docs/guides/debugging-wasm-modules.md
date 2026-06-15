---
title: Debugging WASM Modules
description: Debug module execution, host API calls and sync behavior.
---

This guide covers the tools and techniques for understanding what a Numax module is doing, diagnosing errors, inspecting CRDT state and observing sync behavior. There is no symbolic debugger that attaches to the WASM module, but the runtime exposes enough to make every problem diagnosable.

---

## Logging from the module

The first tool is `nx_log!`. Every string the module sends to the host ends up in the runtime log.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("module started");

    match db::get("my_key").unwrap() {
        Some(v) => nx_log!("found: {:?}", v),
        None    => nx_log!("key not found"),
    }

    nx_log!("module done");
}
```

By default the runtime prints to `stderr` in text format. To increase verbosity:

```bash
nx run my_module.wasm --log-level debug
nx run my_module.wasm --log-level trace   # maximum verbosity, includes runtime internals
```

For structured output (useful for grep, jq, log aggregators):

```bash
nx run my_module.wasm --log-level debug --log-format json
```

The `-v` / `--verbose` flag is a shortcut for `--log-level debug`:

```bash
nx run my_module.wasm -v
```

**Log level precedence:** CLI flag → `NX_LOG_LEVEL` env var → config file → default (`info`).

---

## Reading error codes

Every Host API function returns an `i32`. The SDK converts negative codes to `NxError`, but if you are building custom wrappers or debugging unexpected behavior, these are the codes:

| Code | Constant | Meaning |
|---|---|---|
| `>= 0` | — | success, value = bytes written |
| `-1` | `ERR_NOT_FOUND` | key not found |
| `-2` | `ERR_BUFFER_TOO_SMALL` | buffer too small, SDK retries automatically |
| `-3` | `ERR_INTERNAL` | internal runtime error |
| `-4` | `ERR_RESERVED_KEY` | key in the reserved `__nx/` prefix |
| `-5` | `ERR_SYNC_DISABLED` | CRDT operation requested but sync not enabled |

If you see `ERR_INTERNAL` in the logs, the runtime has already printed the error detail to `stderr`. Look for lines with `[nx-core]`.

---

## Diagnosing common errors

### The module does not start

```
[nx-cli] error: No entrypoint found (expected `run` or `_start`)
```

The module does not export `run`. Check:

```rust
// must be exactly this
#[unsafe(no_mangle)]
pub extern "C" fn run() { ... }
```

And that `Cargo.toml` has:
```toml
[lib]
crate-type = ["cdylib"]
```

### Link error at startup

```
[nx-cli] error: import of `nx::something` was not found
```

The module imports a host function that does not exist in the `nx` namespace. Check that you are using a version of `nx-sdk` compatible with the runtime version.

To see the available capabilities directly from the module:

```rust
let caps = nx_sdk::system::host_capabilities().unwrap();
for cap in &caps {
    nx_log!("{}", cap);
}
```

### `ERR_RESERVED_KEY` on db keys

The `__nx/` prefix is reserved by the runtime. If one of your keys starts with that prefix, rename it. Correct example:

```rust
db::set("app:config:theme", b"dark").unwrap();  // ok
db::set("__nx/config", b"dark").unwrap();        // ERR_RESERVED_KEY
```

### `ERR_SYNC_DISABLED` on CRDT calls

CRDT functions require sync to be enabled with `--listen`. If the module calls `crdt_gcounter_inc` on a standalone node it returns `-5`. Solution:

```bash
nx run my_module.wasm --listen 0.0.0.0:9000
```

Or, if you want to handle both cases in the module:

```rust
use nx_sdk::{crdt::gcounter, NxError};

match gcounter::inc("counter:visits", 1) {
    Ok(()) => {}
    Err(NxError::SyncDisabled) => nx_log!("sync not enabled, skipping CRDT"),
    Err(e) => nx_log!("error: {}", e),
}
```

### The module calls `abort`

```
[nx-cli] error: guest abort: something went wrong
```

The module called `system::abort("something went wrong")`. The runtime terminated the guest and reported the message. Find in the module code where `abort` is called and what condition triggered it.

---

## Inspecting CRDT state after execution

The CLI has `--print-*` flags that print the current value of a CRDT after the module has finished and the sync window has closed. All require `--listen`.

```bash
# GCounter
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-gcounter counter:visits

# PNCounter
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-pncounter inventory:sku-1

# LWW-Register
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-lww-register status:user-1

# LWW-Map
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-lww-map settings:svc-a

# ORSet
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-orset tags:item-1

# RGA
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --settle-for 2s \
  --print-rga comments:doc-1
```

Example output:

```
counter:visits = 42
inventory:sku-1 = 7
status:user-1 = online
settings:svc-a = {theme=dark, region=eu}
tags:item-1 = [blue, red]
comments:doc-1 = [first comment, reply]
```

---

## Inspecting the sync protocol

To make wire messages between nodes human-readable, use `--debug-protocol`. This switches the serialization format from bincode to JSON:

```bash
# Node A
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --debug-protocol \
  --log-level debug

# Node B
nx run my_module.wasm \
  --listen 0.0.0.0:9001 \
  --peer 127.0.0.1:9000 \
  --debug-protocol \
  --log-level debug
```

With `--log-level trace` the runtime logs every network message received and sent. JSON messages are readable directly in the console.

**Note:** `--debug-protocol` is not compatible with nodes using bincode. In a mixed cluster use the same format on all nodes.

---

## Observability endpoint

For a long-running node (`serve()` or with `--listen` without `--settle-for`), enable the HTTP endpoint:

```bash
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --observability-listen 127.0.0.1:9100
```

Three endpoints available:

```bash
# liveness
curl http://127.0.0.1:9100/health
# -> ok

# readiness (503 until the runtime is ready)
curl http://127.0.0.1:9100/ready
# -> ready

# Prometheus metrics
curl http://127.0.0.1:9100/metrics
```

Metrics output:

```
# HELP numax_ops_total Operations processed
# TYPE numax_ops_total counter
numax_ops_total 42
# HELP numax_peers_connected Active peers
# TYPE numax_peers_connected gauge
numax_peers_connected 2
# HELP numax_sync_latency_ms Last sync latency in milliseconds
numax_sync_latency_ms 3
# HELP numax_sync_errors_total Sync errors
numax_sync_errors_total 0
numax_peer_connects_total 5
numax_peer_disconnects_total 1
numax_broadcast_batches_total 12
numax_broadcast_ops_total 48
# HELP numax_store_keys Keys in the local store
numax_store_keys 156
# HELP numax_store_bytes Bytes used by local store keys and values
numax_store_bytes 8192
```

Metrics are in Prometheus-compatible format. You can scrape them with a local Prometheus and visualize in Grafana.

---

## Inspecting effective configuration

Before starting, verify the configuration is what you think it is:

```bash
# generate a commented file with all default values
nx config init --output numax.toml

# validate an existing file without running anything
nx config validate --config numax.toml

# show the effective configuration after applying CLI + env + file + defaults
nx config show --config numax.toml --effective
```

Precedence is: **CLI flags > `NX_*` environment variables > TOML file > defaults**. If a value is not what you expect, `config show --effective` tells you exactly which source won.

---

## Testing convergence behavior with bounded nodes

To test that two nodes converge to the same state without keeping them running indefinitely:

```bash
# Node A - generates an increment and waits for propagation
nx run my_module.wasm \
  --listen 0.0.0.0:9000 \
  --peer 127.0.0.1:9001 \
  --wait-before-run 500ms \
  --settle-for 2s \
  --print-gcounter counter:visits \
  --datastore-path ./node-a-data

# Node B - receives and converges
nx run my_module.wasm \
  --listen 0.0.0.0:9001 \
  --peer 127.0.0.1:9000 \
  --wait-before-run 500ms \
  --settle-for 2s \
  --print-gcounter counter:visits \
  --datastore-path ./node-b-data
```

- `--wait-before-run` waits for peers to connect before running the module
- `--settle-for` keeps sync alive after execution to propagate ops
- both nodes should print the same value of `counter:visits` after convergence

If the values diverge, `--log-level debug` shows which ops were received and applied on each node.

---

## Quick debug checklist

| Symptom | First thing to check |
|---|---|
| Module does not start | `#[unsafe(no_mangle)] pub extern "C" fn run()` present? |
| `ERR_INTERNAL` in logs | Search for `[nx-core]` in stderr |
| `ERR_RESERVED_KEY` | Does the key start with `__nx/`? |
| `ERR_SYNC_DISABLED` | Did you pass `--listen`? |
| CRDT not converging | `--log-level trace` on both nodes |
| Unexpected configuration | `nx config show --effective` |
| Wire protocol unreadable | Add `--debug-protocol` |
| Metrics not available | Did you pass `--observability-listen`? |

---

## Related

- [WASM execution](/concepts/wasm-execution/) - sandbox, entry point and HostState
- [CRDT and state](/concepts/crdt-and-state/) - how ops are applied and propagated
- [Observability](/guides/observability/) - full metrics and health check setup
- [CLI reference](/reference/cli/) - all available flags