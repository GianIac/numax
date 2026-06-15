---
title: Local KV State
description: Step-by-step guide to Numax's local key/value store.
---

This guide walks you through building a WASM module that uses Numax's local key/value store. We start from zero: project setup, basic operations, pagination, and a real persistent counter example. Everything runs on a single node, offline, without sync.

---

## What we build

By the end of this guide you will have:

1. a Rust module compiled to `.wasm` that reads and writes local state
2. understood the read/write/scan pattern of the Host API
3. a persistent counter that survives restarts
4. the foundation for any module that uses local state

---

## Prerequisites

- Rust with the `wasm32-unknown-unknown` target installed
- `nx` CLI installed ([Installation](/numax/getting-started/installation/))

```bash
rustup target add wasm32-unknown-unknown
```

---

## Step 1 - Create the project

```bash
cargo new --lib my_kv_store
cd my_kv_store
```

Edit `Cargo.toml`:

```toml
[package]
name = "my_kv_store"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
nx-sdk = { path = "/path/to/numax/crates/nx-sdk" }

[profile.release]
lto = true
opt-level = "z"
codegen-units = 1
panic = "abort"

[workspace]
```

If you cloned the Numax repository, you can use a relative path. When `nx-sdk` is published on crates.io, `nx-sdk = "0.1"` will be enough.

---

## Step 2 - Write the module

Replace the content of `src/lib.rs`:

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // write a key/value pair
    db::set("hello", b"world").unwrap();
    nx_log!("written: hello = world");

    // read it back
    match db::get("hello").unwrap() {
        Some(bytes) => {
            let s = core::str::from_utf8(&bytes).unwrap_or("?");
            nx_log!("read: hello = {}", s);
        }
        None => nx_log!("key not found"),
    }

    // check existence
    let exists = db::exists("hello").unwrap();
    nx_log!("exists: {}", exists); // true

    // delete
    db::delete("hello").unwrap();
    nx_log!("after delete: {:?}", db::get("hello").unwrap()); // None
}
```

---

## Step 3 - Build and run

```bash
cargo build --target wasm32-unknown-unknown --release

nx run target/wasm32-unknown-unknown/release/my_kv_store.wasm
```

Expected output:

```text
written: hello = world
read: hello = world
exists: true
after delete: None
```

The store is persistent. If you run the module again, it starts from zero because `delete` removed the key during the previous run. If you remove the `delete`, the second run will return `Some(b"world")` from `get("hello")`.

---

## Step 4 - A persistent counter

This is the most common pattern: read a value, modify it, write it back. The counter survives across runs.

```rust
use nx_sdk::{db, nx_log};

const KEY: &str = "counter";

fn parse_u64(bytes: &[u8]) -> u64 {
    core::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0)
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // read the current value (0 if missing)
    let current = match db::get(KEY).unwrap() {
        Some(v) => parse_u64(&v),
        None => 0,
    };

    // increment
    let next = current.saturating_add(1);

    // persist as an ASCII string
    let s = nx_sdk::__alloc::format!("{}", next);
    db::set(KEY, s.as_bytes()).unwrap();

    nx_log!("counter = {}", next);
}
```

Run it multiple times:

```bash
nx run target/wasm32-unknown-unknown/release/my_kv_store.wasm
# counter = 1
nx run target/wasm32-unknown-unknown/release/my_kv_store.wasm
# counter = 2
nx run target/wasm32-unknown-unknown/release/my_kv_store.wasm
# counter = 3
```

The value lives in the runtime data directory (default `./nx-data`). Delete it to reset the counter.

---

## Step 5 - Scan by prefix

The store supports prefix scans. This is useful for records that share a namespace.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // populate a few keys
    db::set("user:1", b"alice").unwrap();
    db::set("user:2", b"bob").unwrap();
    db::set("user:3", b"carol").unwrap();
    db::set("session:abc", b"active").unwrap();

    // full scan for prefix "user:" - returns everything
    let users = db::scan("user:").unwrap();
    nx_log!("users found: {}", users.len()); // 3

    for (key, value) in &users {
        let k = core::str::from_utf8(key).unwrap_or("?");
        let v = core::str::from_utf8(value).unwrap_or("?");
        nx_log!("  {} = {}", k, v);
    }

    // keys only
    let keys = db::keys("user:").unwrap();
    nx_log!("keys: {:?}", keys.len()); // 3

    // "session:" does not appear in a "user:" scan
    let sessions = db::scan("session:").unwrap();
    nx_log!("sessions: {}", sessions.len()); // 1
}
```

`db::scan` and `db::keys` paginate internally with page size 64. For large datasets or explicit pagination control, use `scan_page_after`:

```rust
// first page
let page = db::scan_page_after("user:", None, 10).unwrap();

// next page
let last_key = page.last().map(|(k, _)| k.clone());
let page2 = db::scan_page_after("user:", last_key.as_deref(), 10).unwrap();
```

The cursor prevents rereading keys that were already seen. It is not a snapshot: if you modify the dataset during a scan, new keys may or may not appear depending on lexicographic order.

---

## Step 6 - Composite keys

The most useful pattern for structuring the store is using composite keys with separators. Choose a separator that does not appear in your data (`:` is common).

```rust
use nx_sdk::{db, nx_log};

fn set_user(id: u32, name: &str) {
    let key = nx_sdk::__alloc::format!("user:{}", id);
    db::set(&key, name.as_bytes()).unwrap();
}

fn get_user(id: u32) -> Option<nx_sdk::__alloc::string::String> {
    let key = nx_sdk::__alloc::format!("user:{}", id);
    db::get(&key)
        .unwrap()
        .map(|b| nx_sdk::__alloc::string::String::from_utf8_lossy(&b).into_owned())
}

fn set_score(game: &str, user_id: u32, score: u64) {
    let key = nx_sdk::__alloc::format!("score:{}:{}", game, user_id);
    let val = nx_sdk::__alloc::format!("{}", score);
    db::set(&key, val.as_bytes()).unwrap();
}

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    set_user(1, "alice");
    set_user(2, "bob");
    set_score("chess", 1, 1500);
    set_score("chess", 2, 1200);

    nx_log!("user 1: {:?}", get_user(1));

    // all users
    let users = db::scan("user:").unwrap();
    nx_log!("total users: {}", users.len());

    // all chess scores
    let scores = db::scan("score:chess:").unwrap();
    nx_log!("chess scores: {}", scores.len());
}
```

---

## Common errors

**The key starts with `__nx/`.**
Those keys are reserved for the runtime. If you call `db::set("__nx/something", ...)`, you receive `NxError::ReservedKey`. Always use your own application prefixes.

**The module does not export `run`.**
If you forget `#[unsafe(no_mangle)]` or `pub extern "C"`, Wasmtime cannot find the entry point and the runtime returns an error. Check that the exported name is exactly `run`.

**The target is not `wasm32-unknown-unknown`.**
`cargo build --release` without `--target` compiles for your operating system, not for WASM. The `.wasm` file is not produced.

**You forgot `[workspace]` in `Cargo.toml`.**
If the example is inside a directory that contains a parent Cargo workspace, without `[workspace]` in the module's `Cargo.toml`, Cargo may try to attach the crate to the wrong workspace. Add `[workspace]` to isolate the module.

---

## Data directory

By default, the runtime saves the store in `./nx-data`. You can change it:

```bash
nx run my_kv_store.wasm --datastore-path ./my-data
```

Or in the configuration file:

```toml
[storage]
datastore_path = "./my-data"
```

Each store directory belongs to a single node. Do not share the same directory between multiple Numax processes running at the same time.

---

## Next steps

- Read [CRDT and state](/numax/concepts/crdt-and-state/) to understand when to use replicated CRDT state instead of the local KV store
- See the [`kv_counter`](https://github.com/GianIac/numax/tree/main/examples/kv_counter) example in the repository
- See [`kv_sdk_roundtrip`](https://github.com/GianIac/numax/tree/main/examples/kv_sdk_roundtrip) to see all store APIs in action
- Read [nx-sdk db](/numax/reference/crates/nx-sdk/) for the full function reference
