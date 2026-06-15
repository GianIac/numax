---
title: Cookbook
description: Short copy-paste recipes.
---

This cookbook is a first version. It contains small, practical, copy-pasteable recipes for the most common Numax APIs.

More recipes will be added over time. If you want to propose one, Pull Requests are very welcome: real examples, small snippets and concrete use cases are perfect for this section.

---

## Log from a module

Use `nx_log!` when you want to see what the guest is doing.

```rust
use nx_sdk::nx_log;

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("module started");
    nx_log!("value = {}", 42);
}
```

---

## Save and read local KV state

`db::*` uses the node-local store. It does not replicate between peers.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    db::set("app:greeting", b"hello").unwrap();

    match db::get("app:greeting").unwrap() {
        Some(bytes) => nx_log!("value = {}", core::str::from_utf8(&bytes).unwrap_or("?")),
        None => nx_log!("missing"),
    }
}
```

---

## Persistent local counter

Classic pattern: read, modify, write back.

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
    let current = db::get(KEY).unwrap().map_or(0, |v| parse_u64(&v));
    let next = current.saturating_add(1);
    let value = nx_sdk::__alloc::format!("{}", next);

    db::set(KEY, value.as_bytes()).unwrap();
    nx_log!("counter = {}", next);
}
```

---

## Scan keys by prefix

Use stable prefixes to create small namespaces in the local store.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    db::set("user:1", b"alice").unwrap();
    db::set("user:2", b"bob").unwrap();
    db::set("session:1", b"active").unwrap();

    for (key, value) in db::scan("user:").unwrap() {
        let key = core::str::from_utf8(&key).unwrap_or("?");
        let value = core::str::from_utf8(&value).unwrap_or("?");
        nx_log!("{} = {}", key, value);
    }
}
```

---

## Increment a replicated GCounter

CRDT APIs require sync to be enabled on the runtime (`--listen`). Each node increments its own slot; peers converge when they exchange ops.

```rust
use nx_sdk::{crdt::gcounter, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    gcounter::inc("counter:visits", 1).unwrap();
    let value = gcounter::value("counter:visits").unwrap();
    nx_log!("visits = {}", value);
}
```

---

## User status with LWW-Register

Use an LWW-Register when you want one current value per key.

```rust
use nx_sdk::{crdt::lww_register, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    lww_register::set("status:user-1", b"online").unwrap();

    if let Some(value) = lww_register::get("status:user-1").unwrap() {
        nx_log!("status = {}", core::str::from_utf8(&value).unwrap_or("?"));
    }
}
```

---

## Tags with ORSet

Use ORSet for replicated sets where adds and removes can happen on different nodes.

```rust
use nx_sdk::{crdt::orset, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    orset::add("tags:item-1", "blue").unwrap();
    orset::add("tags:item-1", "urgent").unwrap();

    let tags = orset::elements("tags:item-1").unwrap();
    nx_log!("tags = {:?}", tags);
}
```

---

## Handle disabled sync

Useful when the same module may run standalone or with sync enabled.

```rust
use nx_sdk::{crdt::gcounter, nx_log, NxError};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    match gcounter::inc("counter:visits", 1) {
        Ok(()) => nx_log!("replicated counter updated"),
        Err(NxError::SyncDisabled) => nx_log!("sync disabled, skipping CRDT update"),
        Err(e) => nx_log!("error: {}", e),
    }
}
```

---

## Read a runtime environment variable

The runtime exposes only variables prefixed with `NX_` or `NUMAX_`.

```rust
use nx_sdk::{nx_log, system};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    match system::env_get("NX_REGION").unwrap() {
        Some(bytes) => nx_log!("region = {}", core::str::from_utf8(&bytes).unwrap_or("?")),
        None => nx_log!("NX_REGION not set"),
    }
}
```

---

## Print host capabilities

Useful for debugging and runtime/SDK compatibility.

```rust
use nx_sdk::{nx_log, system};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    for capability in system::host_capabilities().unwrap() {
        nx_log!("capability: {}", capability);
    }
}
```

---

## SHA-256 hash

Crypto functions are executed by the host.

```rust
use nx_sdk::{crypto, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    let digest = crypto::hash_sha256(b"hello numax").unwrap();
    nx_log!("sha256 first byte = {}", digest[0]);
}
```

---

## Related

- [Local KV State](/numax/guides/build-a-kv-store/) - full guide to the local store
- [CRDT and state](/numax/concepts/crdt-and-state/) - convergence and CRDT types
- [Debugging WASM Modules](/numax/guides/debugging-wasm-modules/) - logs, errors and sync
- [nx-sdk](/numax/reference/crates/nx-sdk/) - guest-side wrappers
