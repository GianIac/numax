---
title: nx-sdk
description: Guest SDK for WASM modules.
---

`nx-sdk` is the library you add to your WASM module to call Numax host functions.
It runs inside the `.wasm` binary, not inside the runtime. It is the only crate
in the workspace that targets `wasm32-unknown-unknown` and has no internal workspace dependencies.

It wraps every raw FFI import from `ffi.rs` into safe, ergonomic Rust functions.
You never call `unsafe` directly from your module code.

---

## What it is not

`nx-sdk` has no async, no networking, no OS, no filesystem. It is `no_std` on wasm32.
The `std` feature exists but is disabled by default. The only external thing it touches
is the Numax host via the `nx` namespace WASM imports in `ffi.rs`.

---

## Adding it to your module

```toml
# Cargo.toml inside your module crate
[lib]
crate-type = ["cdylib"]

[dependencies]
nx-sdk = { path = "../crates/nx-sdk" }

[profile.release]
lto = true
opt-level = "z"
codegen-units = 1
panic = "abort"
```

The entry point Numax looks for:

```rust
#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // your module logic here
}
```

---

## Module layout

```
src/
  lib.rs          public API re-exports, no_std cfg, __alloc re-export
  ffi.rs          raw unsafe extern "C" imports from the nx namespace (private)
  error.rs        NxError enum, Result<T> alias
  log.rs          log(), nx_log! macro
  db.rs           local key/value store
  crypto.rs       random_bytes, hash_sha256, hash_blake3
  time.rs         now(), monotonic()
  net.rs          node_id(), peers()
  system.rs       env_get(), module_id(), host_capabilities(), event_emit(), abort()
  crdt/
    mod.rs
    gcounter.rs   inc(), value()
    pncounter.rs  inc(), dec(), value()
    lww_register.rs set(), get()
    lww_map.rs    set(), remove(), get(), contains(), entries()
    orset.rs      add(), remove(), contains(), elements()
    rga.rs        insert_after(), delete(), values()
```

`ffi.rs` is private (`mod ffi` in `lib.rs`). All other modules are `pub mod`.

---

## Error handling

Every fallible SDK function returns `Result<T>` which is `core::result::Result<T, NxError>`.

```rust
pub enum NxError {
    Internal,           // host returned -3
    BufferTooSmall,     // buffer retry cap exceeded
    NotFound,           // host returned -1
    ReservedKey,        // key under __nx/ prefix
    SyncDisabled,       // API requires --listen, sync is off
    UnknownCode(i32),   // unexpected negative return code
}
```

The buffer-too-small retry loop is handled inside the SDK. You never see it.
When the host writes more bytes than the current buffer holds, the SDK doubles the
buffer and retries automatically up to a cap (`MAX_SCAN_BUFFER = 1 MiB` for scan/keys,
`MAX_NET_BUFFER = 1 MiB` for net).

---

## log

```rust
use nx_sdk::log;
use nx_sdk::nx_log;

log("module started");
nx_log!("value = {}", 42);
nx_log!("running on Numax v{}", env!("CARGO_PKG_VERSION"));
```

`log(s)` calls `host_log_v2`. It is best-effort: if the host returns an error, the call
is silently ignored. Use it freely.

`nx_log!` is a macro that formats a string via `alloc::format!` and calls `log`.
It works exactly like `println!` but routes through the host.

---

## db

Local key/value store. Not replicated. Every node has its own copy.

```rust
use nx_sdk::db;

// write
db::set("user:1", b"alice")?;

// read
match db::get("user:1")? {
    Some(bytes) => { /* ... */ }
    None        => { /* key not found */ }
}

// check existence
if db::exists("user:1")? { /* ... */ }

// delete
db::delete("user:1")?;

// full prefix scan (paginated internally, returns all results)
let rows: Vec<(Vec<u8>, Vec<u8>)> = db::scan("user:")?;

// keys only
let keys: Vec<Vec<u8>> = db::keys("user:")?;

// manual pagination with offset cursor (compatibility only - prefer scan_page_after)
let page = db::scan_page("user:", 0, 64)?;

// manual pagination with key cursor (preferred for large key spaces)
let page = db::scan_page_after("user:", None, 64)?;           // first page
let page = db::scan_page_after("user:", Some(last_key), 64)?; // next page

let kpage = db::keys_page("user:", 0, 64)?;
let kpage = db::keys_page_after("user:", None, 64)?;
```

`scan` and `keys` paginate automatically using `scan_page_after` and `keys_page_after`
internally with a page size of 64. Use them when you want all results at once.
Use `scan_page_after` / `keys_page_after` directly when you want explicit pagination.

Keys under the `__nx/` prefix are reserved by the runtime. Accessing them returns `NxError::ReservedKey`.

---

## crdt

CRDT operations. Require sync to be enabled (`--listen`). Return `NxError::SyncDisabled` if not.

### gcounter

Grow-only counter. Use for totals that only increase.

```rust
use nx_sdk::crdt::gcounter;

gcounter::inc("counter:visits", 1)?;
let total: u64 = gcounter::value("counter:visits")?;
```

### pncounter

Positive/negative counter. Use for stock, balances, anything that moves both ways.

```rust
use nx_sdk::crdt::pncounter;

pncounter::inc("inventory:sku-1", 10)?;
pncounter::dec("inventory:sku-1", 3)?;
let available: i64 = pncounter::value("inventory:sku-1")?;
```

### lww_register

Last-writer-wins register. Stores a single byte value per key. Latest timestamp wins.

```rust
use nx_sdk::crdt::lww_register;

lww_register::set("status:user-1", b"online")?;
let status: Option<Vec<u8>> = lww_register::get("status:user-1")?;
```

`get` returns `None` when the register has never been set.

### lww_map

Map where each field is an independent LWW-register. Removes are tombstoned.

```rust
use nx_sdk::crdt::lww_map;

lww_map::set("settings:svc-a", "theme", b"dark")?;
lww_map::remove("settings:svc-a", "region")?;
let val: Option<Vec<u8>>            = lww_map::get("settings:svc-a", "theme")?;
let exists: bool                     = lww_map::contains("settings:svc-a", "theme")?;
let all: Vec<(String, Vec<u8>)>      = lww_map::entries("settings:svc-a")?;
```

`entries` returns only visible (non-tombstoned) fields.

### orset

Observed-remove set of strings. Concurrent adds that were not observed by a remove stay visible.

```rust
use nx_sdk::crdt::orset;

orset::add("tags:item-1", "blue")?;
orset::remove("tags:item-1", "blue")?;
let has_blue: bool        = orset::contains("tags:item-1", "blue")?;
let all_tags: Vec<String> = orset::elements("tags:item-1")?;
```

### rga

Ordered sequence of byte values. Inserts generate stable ids. Deletes tombstone by id.

```rust
use nx_sdk::crdt::rga;

let id       = rga::insert_after("comments:doc-1", None, b"first comment")?;
let reply_id = rga::insert_after("comments:doc-1", Some(&id), b"reply")?;
rga::delete("comments:doc-1", &reply_id)?;
let visible: Vec<Vec<u8>> = rga::values("comments:doc-1")?;
```

`insert_after(key, parent_id, value)`:
- `parent_id = None` inserts at head.
- `parent_id = Some(id)` inserts after the element with that id.
- Returns the new element's id as `String`.

---

## time

```rust
use nx_sdk::time;

let now_ms:     u64 = time::now();        // Unix timestamp in ms
let elapsed_ms: u64 = time::monotonic();  // monotonic ms since runtime start
```

`now()` uses the host wall clock. `monotonic()` is suitable for elapsed time measurement,
not for persisted timestamps.

---

## crypto

```rust
use nx_sdk::crypto;

let nonce: Vec<u8> = crypto::random_bytes(16)?;
let sha: [u8; 32]  = crypto::hash_sha256(b"payload")?;
let b3: [u8; 32]   = crypto::hash_blake3(b"payload")?;
```

`random_bytes(n)` fills a buffer with `n` cryptographically secure bytes from the host.
Maximum: 1 MiB per call.

---

## net

Requires sync to be enabled. Returns `NxError::SyncDisabled` if not.

```rust
use nx_sdk::net;

let id: String      = net::node_id()?;
let peers: Vec<net::Peer> = net::peers()?;

for peer in &peers {
    nx_log!("peer addr={} node_id={}", peer.addr, peer.node_id);
}
```

`Peer` has two fields: `addr: String` and `node_id: String`.

---

## system

```rust
use nx_sdk::system;

// Read an NX_* or NUMAX_* env var from the host
let val: Option<Vec<u8>> = system::env_get("NX_MY_VAR")?;

// Module identifier set by the runtime
let id: String = system::module_id()?;

// List available host capabilities (newline-separated)
let caps: Vec<String> = system::host_capabilities()?;

// Emit a named event to the runtime
system::event_emit("my.event", b"payload")?;

// Abort with a message visible in the host log
system::abort("something went wrong");
```

`abort` is `-> !`. It calls `ffi::abort` and then spins forever on `core::hint::spin_loop()`.
The host converts the FFI call into a Wasmtime trap that terminates the guest.

---

## ffi.rs

`ffi.rs` is the only file that declares raw unsafe host imports. The public wrapper modules contain
the unsafe call sites and translate host return codes into `Result` values:

```rust
#[link(wasm_import_module = "nx")]
unsafe extern "C" {
    pub fn db_get(key_ptr: u32, key_len: u32, out_ptr: u32, out_cap: u32) -> i32;
    pub fn db_set(key_ptr: u32, key_len: u32, val_ptr: u32, val_len: u32) -> i32;
    // ...
    pub fn host_log_v2(msg_ptr: u32, msg_len: u32) -> i32;
    pub fn host_log(msg_ptr: u32, msg_len: u32);  // legacy, kept for compatibility
}
```

`host_log` is kept for backward compatibility with older guest examples. New code uses `host_log_v2`
via `log()`.

All pointer arguments are `u32` (WASM linear memory offsets). The SDK handles casting from Rust references.
The convention is: strings and slices as `(ptr, len)` pairs, output buffers as `(out_ptr, out_cap)`.

---

## How to add a new SDK wrapper (developer guide)

1. Add the raw FFI declaration to `ffi.rs`.
2. Add the safe wrapper in the appropriate module (`db.rs`, `crypto.rs`, etc.).
3. Handle all return codes: `0` for success, negative codes to `NxError` variants.
4. If the output is variable-length, use the doubling buffer retry pattern from `db.rs`/`net.rs`.
5. Re-export from `lib.rs` if it belongs to the top-level public API.
6. Add the host implementation in `nx-core/src/host_api/`.
7. Register it in `nx-core/src/runtime.rs` via `add_to_linker`.

---

## no_std and alloc

`lib.rs` starts with:

```rust
#![cfg_attr(target_arch = "wasm32", no_std)]

pub extern crate alloc as __alloc;
```

On `wasm32`, the crate is `no_std` and uses `alloc` for `Vec`, `String`, `format!`.
On native (e.g. for unit tests or doc examples), `std` is available normally.

`__alloc` is re-exported so macros like `nx_log!` can reference `$crate::__alloc::format!`
without assuming whether the consumer has `std` or not.

---

## Related

Use this page together with the host/runtime docs:

- [Host API](/numax/reference/host-api/) - the host functions this SDK wraps
- [Your First Module](/numax/getting-started/your-first-module/) - end-to-end example using the SDK
- [nx-core crate](/numax/reference/crates/nx-core/) - implements the functions declared in `ffi.rs`
- [Crates overview](/numax/reference/crates/) - where `nx-sdk` fits in the stack
