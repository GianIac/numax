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

**Example (Rust with nx-sdk):**

```rust
use nx_sdk::db;

let value = db::get("my_key")?;
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

**Example:**

```rust
use nx_sdk::db;

db::set("my_key", b"my_value")?;
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

---

### Logging

#### `host_log_v2`

Writes a log message.

```text
fn host_log_v2(level: u32, msg_ptr: u32, msg_len: u32)
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `level` | `u32` | Log level (0=trace, 1=debug, 2=info, 3=warn, 4=error) |
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
- [ ] `db_scan` - Scan by prefix
- [ ] `db_exists` - Check key existence (without reading the value)
- [ ] `db_keys` - List all keys with prefix

### Network
- [ ] `net_send` - Send message to specific peer
- [ ] `net_broadcast` - Broadcast to all peers
- [ ] `net_peers` - List connected peers
- [ ] `net_node_id` - Get own NodeId

### Time
- [ ] `time_now` - Current Unix timestamp (ms)
- [ ] `time_monotonic` - Monotonic clock for measurements

### Crypto
- [ ] `random_bytes` - Secure random number generation
- [ ] `hash_sha256` - SHA-256 hash
- [ ] `hash_blake3` - BLAKE3 hash (faster)

### CRDT
- [ ] `crdt_counter_inc` - Increment GCounter
- [ ] `crdt_counter_get` - Read GCounter value
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
