# Numax Host API

This documentation describes the host functions available to WASM modules running in the Numax runtime.

## Overview

WASM modules communicate with the host through functions imported from the `nx` namespace. These functions allow:

- Reading/writing persistent data
- Logging and debugging
- (Future) Network communication, timers, etc.

## Namespace: `nx`

### Database

#### `db_get`

Reads a value from the local database.

```text
fn db_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `key_ptr` | `u32` | Pointer to the key in WASM memory |
| `key_len` | `u32` | Length of the key in bytes |
| `out_ptr` | `u32` | Pointer to output buffer |
| `out_cap` | `u32` | Output buffer capacity |

**Return:**

| Value | Meaning |
|-------|---------|
| `>= 0` | Length of value read |
| `-1` | Key not found |
| `-2` | Buffer too small (retry with larger buffer) |
| `-3` | Internal error |
| `-4` | Reserved key |

**Example (Rust with nx-sdk):**

```rust
use nx_sdk::db;

let value = db::get("my_key")?;
```

---

#### `db_exists`

Checks whether a key exists without reading the value.

```text
fn db_exists(key_ptr: u32, key_len: u32) -> i32
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `key_ptr` | `u32` | Pointer to the key in WASM memory |
| `key_len` | `u32` | Length of the key in bytes |

**Return:**

| Value | Meaning |
|-------|---------|
| `1` | Key exists |
| `0` | Key does not exist |
| `-3` | Internal error |
| `-4` | Reserved key |

**Example:**

```rust
use nx_sdk::db;

if db::exists("my_key")? {
    // Key is present.
}
```

---

#### `db_set`

Writes a value to the local database.

```text
fn db_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `key_ptr` | `u32` | Pointer to the key |
| `key_len` | `u32` | Length of the key |
| `val_ptr` | `u32` | Pointer to the value |
| `val_len` | `u32` | Length of the value |

**Return:**

| Value | Meaning |
|-------|---------|
| `0` | Success |
| `-3` | Internal error |
| `-4` | Reserved key |

**Example:**

```rust
use nx_sdk::db;

db::set("my_key", b"my_value")?;
```

---

#### `db_scan`

Scans key/value pairs matching a prefix and returns a bounded page.

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

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `prefix_ptr` | `u32` | Pointer to the prefix in WASM memory |
| `prefix_len` | `u32` | Length of the prefix in bytes |
| `cursor` | `u64` | Logical row offset among visible rows matching the prefix |
| `limit` | `u32` | Maximum rows to return, capped by the host |
| `out_ptr` | `u32` | Pointer to output buffer |
| `out_cap` | `u32` | Output buffer capacity |

**Return:**

| Value | Meaning |
|-------|---------|
| `>= 0` | Number of bytes written to `out_ptr` |
| `-2` | Buffer too small (retry with larger buffer) |
| `-3` | Internal error |
| `-4` | Reserved prefix |

**Output encoding:**

```text
u32 row_count
repeat row_count times:
  u32 key_len
  u32 value_len
  u8[key_len] key
  u8[value_len] value
```

All integer fields are little-endian. Runtime-reserved keys under `__nx/` are never returned.

**Example:**

```rust
use nx_sdk::db;

let rows = db::scan("user:")?;
```

---

#### `db_keys`

Lists keys matching a prefix and returns a bounded page.

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

**Parameters:** same as `db_scan`.

**Return:**

| Value | Meaning |
|-------|---------|
| `>= 0` | Number of bytes written to `out_ptr` |
| `-2` | Buffer too small (retry with larger buffer) |
| `-3` | Internal error |
| `-4` | Reserved prefix |

**Output encoding:**

```text
u32 key_count
repeat key_count times:
  u32 key_len
  u8[key_len] key
```

All integer fields are little-endian. Runtime-reserved keys under `__nx/` are never returned.

**Example:**

```rust
use nx_sdk::db;

let keys = db::keys("user:")?;
```

---

#### `db_delete`

Deletes a key from the database.

```text
fn db_delete(key_ptr: u32, key_len: u32) -> i32
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `key_ptr` | `u32` | Pointer to the key |
| `key_len` | `u32` | Length of the key |

**Return:**

| Value | Meaning |
|-------|---------|
| `0` | Success (even if key did not exist) |
| `-3` | Internal error |
| `-4` | Reserved key |

---

### Time

Time functions expose host-managed clocks to WASM modules.

#### `time_now`

Returns the current Unix timestamp in milliseconds.

```text
fn time_now() -> u64
```

**Return:** milliseconds since `1970-01-01T00:00:00Z`.

**Example:**

```rust
use nx_sdk::time;

let now_ms = time::now();
```

---

#### `time_monotonic`

Returns monotonic milliseconds since the runtime process initialized its
monotonic clock. Use this for measuring elapsed durations, not for persisted
timestamps.

```text
fn time_monotonic() -> u64
```

**Return:** monotonic milliseconds relative to the runtime process.

**Example:**

```rust
use nx_sdk::time;

let start = time::monotonic();
// work
let elapsed_ms = time::monotonic() - start;
```

---

### Crypto

Crypto functions expose host-provided randomness and hashing primitives to WASM
modules. All crypto functions are bounded to protect host memory.

#### `random_bytes`

Fills a guest buffer with cryptographically secure random bytes from the host.

```text
fn random_bytes(out_ptr: u32, out_len: u32) -> i32
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `out_ptr` | `u32` | Pointer to output buffer |
| `out_len` | `u32` | Number of random bytes to write |

**Return:**

| Value | Meaning |
|-------|---------|
| `>= 0` | Number of bytes written |
| `-3` | Internal error |

**Example:**

```rust
use nx_sdk::crypto;

let nonce = crypto::random_bytes(16)?;
```

---

#### `hash_sha256`

Computes a 32-byte SHA-256 digest.

```text
fn hash_sha256(input_ptr: u32, input_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

**Return:**

| Value | Meaning |
|-------|---------|
| `32` | Success; digest written to `out_ptr` |
| `-2` | Output buffer too small |
| `-3` | Internal error |

**Example:**

```rust
use nx_sdk::crypto;

let digest = crypto::hash_sha256(b"payload")?;
```

---

#### `hash_blake3`

Computes a 32-byte BLAKE3 digest.

```text
fn hash_blake3(input_ptr: u32, input_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

**Return:** same as `hash_sha256`.

**Example:**

```rust
use nx_sdk::crypto;

let digest = crypto::hash_blake3(b"payload")?;
```

---

### CRDT

CRDT functions operate on replicated data types. In the current implementation,
GCounter state is held in the runtime sync manager's in-memory registry,
persisted as durable CRDT state/op-log metadata, and materialized to sled after
each local or remote update. Operations are broadcast to peers when sync is
enabled and recovered through periodic anti-entropy after reconnects or missed
pushes. On startup, the runtime hydrates the in-memory GCounter registry from
durable CRDT state/op-log data, with materialized totals retained as a fallback.

#### `crdt_gcounter_inc`

Increments a grow-only counter for the local node.

```text
fn crdt_gcounter_inc(key_ptr: u32, key_len: u32, delta: u64) -> i32
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `key_ptr` | `u32` | Pointer to the counter key |
| `key_len` | `u32` | Length of the key in bytes |
| `delta` | `u64` | Non-negative increment amount |

**Return:**

| Value | Meaning |
|-------|---------|
| `0` | Success |
| `-3` | Internal error |
| `-4` | Reserved runtime key |
| `-5` | Sync is disabled |

**Example:**

```rust
use nx_sdk::crdt::gcounter;

gcounter::inc("counter:visits", 1)?;
```

---

#### `crdt_gcounter_value`

Reads the current runtime value of a grow-only counter.

```text
fn crdt_gcounter_value(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32
```

The output is an 8-byte little-endian `u64`.

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `key_ptr` | `u32` | Pointer to the counter key |
| `key_len` | `u32` | Length of the key in bytes |
| `out_ptr` | `u32` | Pointer to an 8-byte output buffer |
| `out_cap` | `u32` | Output buffer capacity |

**Return:**

| Value | Meaning |
|-------|---------|
| `8` | Success; 8 bytes written |
| `-2` | Buffer too small |
| `-3` | Internal error |
| `-4` | Reserved runtime key |
| `-5` | Sync is disabled |

**Example:**

```rust
use nx_sdk::crdt::gcounter;

let visits = gcounter::value("counter:visits")?;
```

---

### Logging

#### `host_log_v2`

Writes a log message.

```text
fn host_log_v2(msg_ptr: u32, msg_len: u32) -> i32
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `msg_ptr` | `u32` | Pointer to the message |
| `msg_len` | `u32` | Length of the message |

**Example:**

```rust
use nx_sdk::log;

log("Hello from WASM!");
```

---

## Limits

| Resource | Limit |
|----------|-------|
| Key length | 1024 bytes |
| Value length | 1 MB |
| Output buffer | 10 MB |

---

## Error Codes

| Code | Constant | Description |
|------|----------|-------------|
| `0` | `OK` | Success |
| `-1` | `ERR_NOT_FOUND` | Key not found |
| `-2` | `ERR_BUFFER_TOO_SMALL` | Output buffer insufficient |
| `-3` | `ERR_INTERNAL` | Internal error |
| `-4` | `ERR_RESERVED_KEY` | Key uses a runtime-reserved prefix |
| `-5` | `ERR_SYNC_DISABLED` | CRDT sync is not enabled |

---

## Full Example

```rust
#![no_std]

use nx_sdk::{db, log};

#[no_mangle]
pub extern "C" fn run() {
    log("Starting module...");
    
    // Write a value
    db::set("counter", b"0").unwrap();
    
    // Read the value
    if let Ok(Some(val)) = db::get("counter") {
        log(&format!("Counter: {:?}", val));
    }
    
    // Delete
    db::delete("counter").unwrap();
    
    log("Module completed!");
}
```

---

## Roadmap
> It may vary, we are still in development

### Database
- [x] `db_scan` - Scan by prefix
- [x] `db_exists` - Check key existence (without reading the value)
- [x] `db_keys` - List all keys with prefix

### Network
- [ ] `net_send` - Send message to specific peer
- [ ] `net_broadcast` - Broadcast to all peers
- [ ] `net_peers` - List connected peers
- [ ] `net_node_id` - Get own NodeId

### Time
- [x] `time_now` - Current Unix timestamp (ms)
- [x] `time_monotonic` - Monotonic clock for measurements

### Crypto
- [x] `random_bytes` - Secure random number generation
- [x] `hash_sha256` - SHA-256 hash
- [x] `hash_blake3` - BLAKE3 hash (faster)

### CRDT
- [x] `crdt_gcounter_inc` - Increment GCounter
- [x] `crdt_gcounter_value` - Read GCounter value
- [x] Durable GCounter materialization in sled
- [x] Durable GCounter CRDT state/op-log metadata
- [x] Startup hydration from durable CRDT state/op-log data
- [x] Bounded OpId dedup metadata persisted across restart
- [ ] `crdt_set_add` - Add element to ORSet
- [ ] `crdt_set_remove` - Remove element from ORSet
- [ ] `crdt_set_contains` - Check membership

### System
- [ ] `env_get` - Read environment variable
- [ ] `module_id` - Get current module ID
- [ ] `abort` - Terminate execution with error

### Events (Callbacks)
- [ ] `on_peer_connect` - Callback when a peer connects
- [ ] `on_peer_disconnect` - Callback when a peer disconnects
- [ ] `on_message` - Message reception callback
- [ ] `on_timer` - Periodic timer
