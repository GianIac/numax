---
title: Host API
description: Reference for host functions exposed to WASM modules.
---


This document describes the host functions available to WASM modules running in
the Numax runtime.

All functions are imported from the `nx` namespace. Most users should call them
through `nx-sdk`; the raw ABI is documented here so module authors can understand
the wire contract and future SDK bindings can stay consistent.

## Contents

- [ABI Conventions](#abi-conventions)
- [Error Codes](#error-codes)
- [Limits](#limits)
- [Database API](#database-api)
- [CRDT API](#crdt-api)
- [Time API](#time-api)
- [Crypto API](#crypto-api)
- [System API](#system-api)
- [Network API](#network-api)
- [Logging API](#logging-api)
- [Roadmap](#roadmap)

---

## ABI Conventions

### Namespace

All imports use the `nx` namespace.

### Memory

Strings, keys and byte arrays are passed as `(ptr, len)` pairs into guest linear
memory. Output functions write into `(out_ptr, out_cap)` buffers and return the
number of bytes written.

### Return Values

Unless noted otherwise:

- `0` means success for command-style functions.
- `> 0` means the number of bytes written for read-style functions.
- negative values are error codes listed in [Error Codes](#error-codes).

### Integer Encoding

Structured binary outputs use little-endian integer fields unless explicitly
stated otherwise.

### Reserved Keys

Keys under the runtime-reserved `__nx/` prefix are not visible to guest modules.
Database APIs reject direct access to reserved keys/prefixes.

---

## Error Codes

| Code | Constant | Meaning |
|------|----------|---------|
| `0` | `OK` | Success |
| `-1` | `ERR_NOT_FOUND` | Key/value/resource was not found |
| `-2` | `ERR_BUFFER_TOO_SMALL` | Output buffer is too small |
| `-3` | `ERR_INTERNAL` | Host/runtime error |
| `-4` | `ERR_RESERVED_KEY` | Key or prefix uses a runtime-reserved namespace |
| `-5` | `ERR_SYNC_DISABLED` | Sync-dependent API was called without sync enabled |

---

## Limits

| Resource | Limit |
|----------|-------|
| Key length | 1024 bytes |
| Value length | 1 MiB |
| Output buffer | 10 MiB |
| `random_bytes` request | 1 MiB |
| Log message | 8 KiB |

The SDK mirrors the important host-side limits where possible, so oversized
guest allocations can fail before calling into the host.

---

## Database API

Database functions operate on the local embedded key/value store. They do not
replicate by themselves. Use the CRDT API for replicated state.

### Summary

| Function | Purpose | Status |
|----------|---------|--------|
| `db_get` | Read value by key | Implemented |
| `db_set` | Write value by key | Implemented |
| `db_delete` | Delete key | Implemented |
| `db_exists` | Check key existence | Implemented |
| `db_scan` | Prefix scan with offset cursor | Implemented, compatibility |
| `db_scan_after` | Prefix scan with key cursor | Implemented, preferred |
| `db_keys` | Prefix key listing with offset cursor | Implemented, compatibility |
| `db_keys_after` | Prefix key listing with key cursor | Implemented, preferred |

### `db_get`

```text
fn db_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

Returns value length on success, `ERR_NOT_FOUND` when absent, or
`ERR_BUFFER_TOO_SMALL` when `out_cap` is insufficient.

SDK:

```rust
use nx_sdk::db;

let value = db::get("user:1")?;
```

### `db_set`

```text
fn db_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32
```

Returns `0` on success.

SDK:

```rust
use nx_sdk::db;

db::set("user:1", b"alice")?;
```

### `db_delete`

```text
fn db_delete(key_ptr: u32, key_len: u32) -> i32
```

Returns `0` on success, even if the key did not exist.

### `db_exists`

```text
fn db_exists(key_ptr: u32, key_len: u32) -> i32
```

Returns:

| Value | Meaning |
|-------|---------|
| `1` | Key exists |
| `0` | Key does not exist |
| `< 0` | Error |

SDK:

```rust
use nx_sdk::db;

if db::exists("user:1")? {
    // present
}
```

### `db_scan`

```text
fn db_scan(
    prefix_ptr: u32,
    prefix_len: u32,
    cursor: u64,
    limit: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Returns a bounded page of key/value pairs matching `prefix`.

`cursor` is a logical row offset among visible rows. This API is kept for
compatibility; new modules should prefer `db_scan_after` because offset cursors
can shift when the store changes during pagination.

Output encoding:

```text
u32 row_count
repeat row_count times:
  u32 key_len
  u32 value_len
  u8[key_len] key
  u8[value_len] value
```

### `db_scan_after`

```text
fn db_scan_after(
    prefix_ptr: u32,
    prefix_len: u32,
    start_after_ptr: u32,
    start_after_len: u32,
    limit: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Preferred scan API for large key spaces. `start_after_len = 0` starts from the
first visible key. Otherwise, `start_after` must be a key under `prefix`.

Output encoding is the same as `db_scan`.

SDK:

```rust
use nx_sdk::db;

let rows = db::scan("user:")?;
```

### `db_keys`

```text
fn db_keys(
    prefix_ptr: u32,
    prefix_len: u32,
    cursor: u64,
    limit: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Lists keys matching `prefix` using an offset cursor. Kept for compatibility; new
modules should prefer `db_keys_after`.

Output encoding:

```text
u32 key_count
repeat key_count times:
  u32 key_len
  u8[key_len] key
```

### `db_keys_after`

```text
fn db_keys_after(
    prefix_ptr: u32,
    prefix_len: u32,
    start_after_ptr: u32,
    start_after_len: u32,
    limit: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Preferred key-listing API for large key spaces. `start_after_len = 0` starts
from the first visible key. Output encoding is the same as `db_keys`.

SDK:

```rust
use nx_sdk::db;

let keys = db::keys("user:")?;
```

---

## CRDT API

CRDT functions operate on replicated state managed by the runtime sync manager.
They require sync to be enabled; otherwise they return `ERR_SYNC_DISABLED`.

CRDT state is:

- held in an in-memory registry while the runtime is alive;
- persisted as durable CRDT state and op-log metadata;
- materialized into the local store for restart/readback;
- broadcast to peers through the sync layer;
- repaired through reconnect and anti-entropy when peers miss pushes.

### Completion Rule For New CRDTs

A CRDT host API is considered complete only when it has:

- implementation in `nx-sync`;
- `OpKind` and serialization support;
- durable state/op-log integration in `nx-core`;
- host API and SDK wrapper;
- unit/property tests;
- SyncManager E2E test;
- distributed example with a README and reproducible 2-3 node run.

### Summary

| CRDT | Functions | Status |
|------|-----------|--------|
| GCounter | `crdt_gcounter_inc`, `crdt_gcounter_value` | Implemented |
| PNCounter | `crdt_pncounter_inc`, `crdt_pncounter_dec`, `crdt_pncounter_value` | Implemented |
| LWW-Register | `crdt_lww_set`, `crdt_lww_get` | Implemented |
| ORSet | `crdt_orset_add`, `crdt_orset_remove`, `crdt_orset_contains`, `crdt_orset_elements` | Implemented |
| LWW-Map | `crdt_lww_map_set`, `crdt_lww_map_remove`, `crdt_lww_map_get`, `crdt_lww_map_contains`, `crdt_lww_map_entries` | Implemented |
| RGA | `crdt_rga_insert`, `crdt_rga_delete`, `crdt_rga_values` | Implemented |

### GCounter

A grow-only counter. Each node owns its own positive slot. The converged value is
the sum of all node slots.

Use it for totals that only increase: visits, emitted events, successful jobs.

#### `crdt_gcounter_inc`

```text
fn crdt_gcounter_inc(key_ptr: u32, key_len: u32, delta: u64) -> i32
```

Returns `0` on success.

SDK:

```rust
use nx_sdk::crdt::gcounter;

gcounter::inc("counter:visits", 1)?;
```

#### `crdt_gcounter_value`

```text
fn crdt_gcounter_value(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Writes an 8-byte little-endian `u64`. Returns `8` on success.

SDK:

```rust
use nx_sdk::crdt::gcounter;

let visits = gcounter::value("counter:visits")?;
```

### PNCounter

A positive/negative counter. It supports increments and decrements while still
converging without coordination. Internally it is expected to behave like two
grow-only counters: one for positive slots and one for negative slots.

Use it for stock, balances, votes with up/down movement, or any value that must
move both directions while tolerating eventual consistency.

#### `crdt_pncounter_inc`

```text
fn crdt_pncounter_inc(key_ptr: u32, key_len: u32, delta: u64) -> i32
```

Returns `0` on success.

SDK:

```rust
use nx_sdk::crdt::pncounter;

pncounter::inc("inventory:sku-1", 10)?;
```

#### `crdt_pncounter_dec`

```text
fn crdt_pncounter_dec(key_ptr: u32, key_len: u32, delta: u64) -> i32
```

Returns `0` on success.

SDK:

```rust
use nx_sdk::crdt::pncounter;

pncounter::dec("inventory:sku-1", 3)?;
```

#### `crdt_pncounter_value`

```text
fn crdt_pncounter_value(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Writes a signed 8-byte little-endian `i64`. Returns `8` on success.

SDK:

```rust
use nx_sdk::crdt::pncounter;

let available = pncounter::value("inventory:sku-1")?;
```

### LWW-Register

A last-writer-wins register stores a single byte value per key. Each write is
tagged by the host with a timestamp and the local `NodeId`; higher timestamps
win, and equal timestamps are resolved deterministically by `NodeId`.

The host keeps local writes monotonic against the currently observed register
state, so repeated writes from the same module can still replace earlier local
values even when they happen within the same millisecond.

Use it for statuses, labels, configuration values, selected modes, or other
single-value state where "latest known value wins" is the right conflict policy.

#### `crdt_lww_set`

```text
fn crdt_lww_set(
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32
) -> i32
```

Returns `0` on success. Values are bounded to 1 MiB.

SDK:

```rust
use nx_sdk::crdt::lww_register;

lww_register::set("status:user-1", b"online")?;
```

#### `crdt_lww_get`

```text
fn crdt_lww_get(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Writes the current winning value into `out_ptr` and returns the number of bytes
written. Returns `ERR_NOT_FOUND` when the register has no value and
`ERR_BUF_TOO_SMALL` when `out_cap` is not large enough.

SDK:

```rust
use nx_sdk::crdt::lww_register;

let status = lww_register::get("status:user-1")?;
```

### LWW-Map

A key-value map where each field is an independent last-writer-wins register.
Removes are stored as tombstones, so old writes cannot resurrect deleted fields
after reconnect or anti-entropy replay.

Use it for replicated settings, feature flags, service metadata, and small
configuration documents.

#### `crdt_lww_map_set`

```text
fn crdt_lww_map_set(
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32,
    value_ptr: u32,
    value_len: u32
) -> i32
```

Sets one field. The host assigns the timestamp and local writer NodeId.

SDK:

```rust
use nx_sdk::crdt::lww_map;

lww_map::set("settings:service-a", "theme", b"dark")?;
```

#### `crdt_lww_map_remove`

```text
fn crdt_lww_map_remove(
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32
) -> i32
```

Removes one field by writing a tombstone.

SDK:

```rust
use nx_sdk::crdt::lww_map;

lww_map::remove("settings:service-a", "region")?;
```

#### `crdt_lww_map_get`

```text
fn crdt_lww_map_get(
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Writes the visible field value and returns the number of bytes written. Returns
`ERR_NOT_FOUND` when the map or field is absent, including tombstoned fields.

SDK:

```rust
use nx_sdk::crdt::lww_map;

let value = lww_map::get("settings:service-a", "theme")?;
```

#### `crdt_lww_map_contains`

```text
fn crdt_lww_map_contains(
    key_ptr: u32,
    key_len: u32,
    field_ptr: u32,
    field_len: u32
) -> i32
```

Returns `1` when the field has a visible value and `0` when it is absent or
tombstoned.

SDK:

```rust
use nx_sdk::crdt::lww_map;

let has_theme = lww_map::contains("settings:service-a", "theme")?;
```

#### `crdt_lww_map_entries`

```text
fn crdt_lww_map_entries(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Writes visible entries in deterministic field order and returns the number of
bytes written. Tombstones are not returned. The raw output encoding is:

```text
u32 entry_count
repeat entry_count times:
  u32 field_len
  u8[field_len] utf8_field
  u32 value_len
  u8[value_len] value
```

SDK:

```rust
use nx_sdk::crdt::lww_map;

let entries = lww_map::entries("settings:service-a")?;
```

### ORSet

An observed-remove set stores visible string elements per key. Each add creates
a unique add-tag. A remove carries the add-tags observed locally for that
element, so concurrent adds that were not observed by the remove remain visible
after merge.

Use it for tags, labels, feature sets, membership, or other string sets where
adds/removes must converge without coordination.

#### `crdt_orset_add`

```text
fn crdt_orset_add(
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32
) -> i32
```

Returns `0` on success. The host generates the add-tag from the local `OpId`.

SDK:

```rust
use nx_sdk::crdt::orset;

orset::add("tags:item-1", "blue")?;
```

#### `crdt_orset_remove`

```text
fn crdt_orset_remove(
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32
) -> i32
```

Returns `0` on success. Removing an element that has no locally observed add-tags
is a no-op.

SDK:

```rust
use nx_sdk::crdt::orset;

orset::remove("tags:item-1", "blue")?;
```

#### `crdt_orset_contains`

```text
fn crdt_orset_contains(
    key_ptr: u32,
    key_len: u32,
    element_ptr: u32,
    element_len: u32
) -> i32
```

Returns `1` when the element is visible and `0` when it is absent.

SDK:

```rust
use nx_sdk::crdt::orset;

let has_blue = orset::contains("tags:item-1", "blue")?;
```

#### `crdt_orset_elements`

```text
fn crdt_orset_elements(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Writes visible elements in deterministic order and returns the number of bytes
written. The raw output encoding is:

```text
u32 element_count
repeat element_count times:
  u32 element_len
  u8[element_len] utf8_element
```

SDK:

```rust
use nx_sdk::crdt::orset;

let tags = orset::elements("tags:item-1")?;
```

### RGA

An RGA stores an ordered sequence of byte values per key. Inserts create stable
element ids and optionally point at a parent element id. Deletes tombstone an
element id, so children inserted after a deleted element remain visible and
ordered.

Use it for ordered comments, collaborative text/list building blocks, workflow
logs, or any append/insert-after sequence that must converge without
coordination.

#### `crdt_rga_insert`

```text
fn crdt_rga_insert(
    key_ptr: u32,
    key_len: u32,
    parent_ptr: u32,
    parent_len: u32,
    value_ptr: u32,
    value_len: u32,
    out_id_ptr: u32,
    out_id_cap: u32
) -> i32
```

Inserts `value` after `parent`. Use `parent_len = 0` to insert at the head. The
host generates the element id from the local `OpId`, writes it to `out_id_ptr`,
and returns the number of id bytes written.

SDK:

```rust
use nx_sdk::crdt::rga;

let id = rga::insert_after("comments:doc-1", None, b"first comment")?;
let reply_id = rga::insert_after("comments:doc-1", Some(&id), b"reply")?;
```

#### `crdt_rga_delete`

```text
fn crdt_rga_delete(
    key_ptr: u32,
    key_len: u32,
    id_ptr: u32,
    id_len: u32
) -> i32
```

Tombstones the element identified by `id` and returns `0` on success.

SDK:

```rust
use nx_sdk::crdt::rga;

rga::delete("comments:doc-1", &reply_id)?;
```

#### `crdt_rga_values`

```text
fn crdt_rga_values(
    key_ptr: u32,
    key_len: u32,
    out_ptr: u32,
    out_cap: u32
) -> i32
```

Writes visible values in deterministic sequence order and returns the number of
bytes written. Tombstoned elements are not returned. The raw output encoding is:

```text
u32 value_count
repeat value_count times:
  u32 value_len
  u8[value_len] value
```

SDK:

```rust
use nx_sdk::crdt::rga;

let comments = rga::values("comments:doc-1")?;
```

### Distributed CRDT Examples

| CRDT | Example | Notes |
|------|---------|-------|
| PNCounter | `examples/distributed_inventory` | Increment/decrement inventory |
| LWW-Register | `examples/distributed_status` | Single status value |
| ORSet | `examples/distributed_tags` | Observed-remove tags |
| LWW-Map | `examples/distributed_settings` | Per-field settings |
| RGA | `examples/distributed_comments` | Ordered comments |

---

## Time API

### `time_now`

```text
fn time_now() -> u64
```

Returns the current Unix timestamp in milliseconds.

SDK:

```rust
use nx_sdk::time;

let now_ms = time::now();
```

### `time_monotonic`

```text
fn time_monotonic() -> u64
```

Returns monotonic milliseconds relative to the runtime process. Use it for
elapsed-time measurement, not persisted wall-clock timestamps.

SDK:

```rust
use nx_sdk::time;

let start = time::monotonic();
let elapsed_ms = time::monotonic() - start;
```

---

## Crypto API

Crypto APIs expose host-provided randomness and hashing primitives.

### `random_bytes`

```text
fn random_bytes(out_ptr: u32, out_len: u32) -> i32
```

Fills `out_ptr` with cryptographically secure random bytes. Returns the number
of bytes written.

SDK:

```rust
use nx_sdk::crypto;

let nonce = crypto::random_bytes(16)?;
```

### `hash_sha256`

```text
fn hash_sha256(input_ptr: u32, input_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

Computes a 32-byte SHA-256 digest. Returns `32` on success.

### `hash_blake3`

```text
fn hash_blake3(input_ptr: u32, input_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

Computes a 32-byte BLAKE3 digest. Returns `32` on success.

SDK:

```rust
use nx_sdk::crypto;

let sha = crypto::hash_sha256(b"payload")?;
let b3 = crypto::hash_blake3(b"payload")?;
```

---

## System API

### `env_get`

```text
fn env_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

Reads an allowed host environment variable. Current policy exposes only
uppercase variables whose names start with `NX_` or `NUMAX_`.

### `module_id`

```text
fn module_id(out_ptr: u32, out_cap: u32) -> i32
```

Returns the current module identifier provided by the runtime.

### `host_capabilities`

```text
fn host_capabilities(out_ptr: u32, out_cap: u32) -> i32
```

Returns UTF-8 capability names separated by `\n`.

### `event_emit`

```text
fn event_emit(name_ptr: u32, name_len: u32, payload_ptr: u32, payload_len: u32) -> i32
```

Emits a named event to the runtime. Event names must be non-empty ASCII names
using letters, digits, `_`, `-`, `.` or `:`.

### `abort`

```text
fn abort(msg_ptr: u32, msg_len: u32)
```

Terminates guest execution with a host-visible error message. The host turns
this call into a Wasmtime trap.

---

## Network API

Network APIs expose sync runtime introspection. They require sync to be enabled.

### `net_node_id`

```text
fn net_node_id(out_ptr: u32, out_cap: u32) -> i32
```

Returns the local sync `NodeId`.

### `net_peers`

```text
fn net_peers(out_ptr: u32, out_cap: u32) -> i32
```

Returns currently connected sync peers.

Output encoding:

```text
u32 peer_count
repeat peer_count times:
  u32 addr_len
  u32 node_id_len
  u8[addr_len] addr
  u8[node_id_len] node_id
```

SDK:

```rust
use nx_sdk::net;

let node_id = net::node_id()?;
let peers = net::peers()?;
```

---

## Logging API

### `host_log_v2`

```text
fn host_log_v2(msg_ptr: u32, msg_len: u32) -> i32
```

Writes a guest log message to the host log stream. Returns `0` on success or
`ERR_INTERNAL` if the message cannot be read from guest memory.

SDK:

```rust
use nx_sdk::log;

log("Hello from WASM!");
```

---

## Full Example

```rust
use nx_sdk::{db, log};

#[no_mangle]
pub extern "C" fn run() {
    log("Starting module...");

    db::set("counter", b"0").unwrap();

    if let Ok(Some(value)) = db::get("counter") {
        log(&format!("Counter: {:?}", value));
    }

    db::delete("counter").unwrap();
    log("Module completed!");
}
```

---

## Roadmap

This section is only a compact API-surface tracker. The authoritative project
roadmap lives in [Roadmap](/numax/roadmap/).

### Implemented

- Database: `db_get`, `db_set`, `db_delete`, `db_exists`, `db_scan`,
  `db_scan_after`, `db_keys`, `db_keys_after`
- CRDT: `crdt_gcounter_inc`, `crdt_gcounter_value`, `crdt_pncounter_inc`,
  `crdt_pncounter_dec`, `crdt_pncounter_value`, `crdt_lww_set`,
  `crdt_lww_get`, `crdt_lww_map_set`, `crdt_lww_map_remove`,
  `crdt_lww_map_get`, `crdt_lww_map_contains`, `crdt_lww_map_entries`,
  `crdt_orset_add`, `crdt_orset_remove`, `crdt_orset_contains`,
  `crdt_orset_elements`, `crdt_rga_insert`, `crdt_rga_delete`,
  `crdt_rga_values`
- Time: `time_now`, `time_monotonic`
- Crypto: `random_bytes`, `hash_sha256`, `hash_blake3`
- System: `env_get`, `module_id`, `abort`, `host_capabilities`, `event_emit`
- Network introspection: `net_node_id`, `net_peers`

### Planned

- Network messaging callbacks/events: `on_peer_connect`, `on_peer_disconnect`,
  `on_message`, `on_timer`
- Optional HTTP/client APIs remain out of scope until a capability model is
  formalized.
