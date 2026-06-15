---
title: WASM execution
description: How WebAssembly modules execute inside Numax.
---

A Numax node runs one WASM module per invocation. The module is a portable binary compiled to the `wasm32-unknown-unknown` or `wasm32-wasip1` target. The runtime loads it, validates it, links the Host API, instantiates it and calls the entry point. This page explains exactly what happens at each step.

---

## What a WASM module is in Numax

A Numax module is a standard WebAssembly binary with one requirement: it must export a function named `run` or `_start` with the signature `() -> ()`.

```rust
#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // application logic
}
```

The module is compiled with `crate-type = ["cdylib"]` and built for `wasm32-unknown-unknown`. It is a self-contained binary: no dynamic linking, no implicit access to the operating system, no network sockets. The only way it can interact with the outside world is by calling functions the host explicitly exposes.

---

## The sandbox

The WASM sandbox is structural, not configured. The module runs inside a Wasmtime `Store` that is created fresh for every invocation of `run_module`. The sandbox enforces:

- **Memory isolation** - the module has access only to its own linear memory. It cannot read or write memory belonging to the host or to other modules.
- **No implicit I/O** - there is no filesystem access, no network stack, no OS calls, unless the host explicitly provides them via WASI or the Host API.
- **Controlled imports** - the module can only call functions that were registered in the `Linker` before instantiation. Any import that is not registered causes a link error at instantiation time, before any code runs.
- **Optional memory cap** - `RuntimeConfig.max_memory_bytes` sets a per-invocation limit enforced by Wasmtime's `StoreLimits`. If the module tries to grow its linear memory beyond this limit, the grow fails.

```rust
// from runtime.rs - per-invocation limits
let mut limits_builder = StoreLimitsBuilder::new();
if let Some(max_bytes) = self.config.max_memory_bytes {
    limits_builder = limits_builder.memory_size(max_bytes as usize);
}
let limits = limits_builder.build();
// ...
store.limiter(|state| &mut state.limits);
```

---

## Compilation and caching

When `run_module(wasm_bytes)` is called:

1. The blake3 hash of the bytes is computed.
2. The module cache (`Mutex<HashMap<[u8; 32], Module>>`) is checked. If a compiled module for this hash exists, it is reused.
3. If not, `Module::new(&engine, wasm_bytes)` compiles and validates the binary. Wasmtime performs full structural and type validation at this point. Invalid modules are rejected before instantiation.
4. The compiled module is inserted into the cache under its hash.

The cache lives for the lifetime of the `Runtime`. Running the same binary a thousand times costs one compilation. Different binaries with the same hash (impossible with blake3) would share a module, but in practice each distinct binary gets its own cache entry.

---

## The Host API: what the module can import

Every host function available to the module is registered in `Runtime::new` via the `add_to_linker` functions in `nx-core/src/host_api/`. The namespace is always `"nx"`. The module imports from this namespace using the FFI declarations in `ffi.rs`.

All 41 host functions registered at startup:

| Group | Functions |
|---|---|
| **db** | `db_get`, `db_set`, `db_delete`, `db_exists`, `db_scan`, `db_scan_after`, `db_keys`, `db_keys_after` |
| **crdt** | `crdt_gcounter_inc`, `crdt_gcounter_value`, `crdt_pncounter_inc`, `crdt_pncounter_dec`, `crdt_pncounter_value`, `crdt_lww_set`, `crdt_lww_get`, `crdt_lww_map_set`, `crdt_lww_map_remove`, `crdt_lww_map_get`, `crdt_lww_map_contains`, `crdt_lww_map_entries`, `crdt_orset_add`, `crdt_orset_remove`, `crdt_orset_contains`, `crdt_orset_elements`, `crdt_rga_insert`, `crdt_rga_delete`, `crdt_rga_values` |
| **log** | `host_log`, `host_log_v2` |
| **time** | `time_now`, `time_monotonic` |
| **crypto** | `random_bytes`, `hash_sha256`, `hash_blake3` |
| **system** | `env_get`, `module_id`, `host_capabilities`, `event_emit`, `abort` |
| **net** | `net_node_id`, `net_peers` |

If the module tries to import a function that is not in this list, instantiation fails with a link error. There is no way to call unlisted host functions.

A module can query the full list of available functions at runtime:

```rust
let caps: Vec<String> = system::host_capabilities()?;
```

---

## Linear memory and the ABI

WASM linear memory is a flat array of bytes. The module and the host share access to it: the module writes data into it, then passes pointers and lengths to host functions. Host functions read from it and write results back into it.

Most host functions follow the same ABI convention:

```
input:   (ptr: u32, len: u32)  ->  host reads len bytes from linear memory at offset ptr
output:  (out_ptr: u32, out_cap: u32)  ->  host writes result into linear memory at out_ptr, up to out_cap bytes
return:  i32  ->  byte count/status on success, negative error code on failure
```

There are a few special cases: `time_now` and `time_monotonic` return `u64` directly,
legacy `host_log` returns `()`, and `abort` traps instead of returning normally.

The host validates every pointer access before touching memory. If the requested range falls outside the module's current linear memory, the call returns `ERR_INTERNAL`. If the output buffer is too small for the result, the call returns `ERR_BUF_TOO_SMALL` and the SDK retries with a larger buffer.

**Input limits enforced by the host:**

| Resource | Limit |
|---|---|
| Key length | 8 KiB |
| Value length | 1 MiB |
| Output buffer capacity | 1 MiB |
| Scan limit per page | 1024 entries |
| Event name length | 128 bytes |
| Event payload | 64 KiB |
| Abort message | 8 KiB |

These limits are checked on every call, before any store or network operation. A module that sends an oversized key receives `ERR_INTERNAL` immediately.

---

## WASI

When `RuntimeConfig.enable_wasi` is `true` (the default), the runtime also links WASI preview1 functions into the module. WASI gives the module access to:

- standard I/O (`stdin`, `stdout`, `stderr`)
- command-line arguments

Numax does not explicitly inherit filesystem handles or WASI environment variables. Runtime environment access goes through `env_get`, which only exposes allowed `NX_*` and `NUMAX_*` variables. The WASI context is built with `WasiCtx::builder().inherit_stdio().inherit_args().build_p1()`, which is the minimum needed to support modules that use `println!` or `eprintln!`.

To disable WASI entirely (for pure logic modules that do not need stdio), set `enable_wasi: false` in `RuntimeConfig`. The module will be linked without WASI functions and any import from the `wasi_snapshot_preview1` namespace will cause a link error.

---

## HostState: what the module sees

Each invocation creates one `HostState` and attaches it to the Wasmtime `Store`. This is the bridge between the module's calls and the runtime's state.

```rust
pub struct HostState {
    pub wasi:        Option<p1::WasiP1Ctx>,  // None when WASI disabled
    pub store:       Arc<NxStore>,           // shared with Runtime and SyncManager
    pub sync_handle: Option<SyncHandle>,     // None when sync disabled
    pub module_id:   Arc<str>,               // set from RuntimeConfig.module_id
    pub limits:      wasmtime::StoreLimits,  // per-invocation memory cap
}
```

The `store` and `sync_handle` are `Arc` clones from the `Runtime`. The sled database is shared: writes from the module are immediately visible to the sync manager. The `sync_handle` is `None` when the runtime was started without `--listen`, which is why CRDT host functions check for it and return `ERR_SYNC_DISABLED` when it is absent.

---

## The entry point

After instantiation, the runtime looks for the entry point in this order:

1. `run` - preferred, explicit Numax entry point
2. `_start` - fallback for WASI-compiled modules

```rust
let run = instance
    .get_typed_func::<(), ()>(&mut store, "run")
    .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "_start"))
    .map_err(|e| anyhow!("No entrypoint found (expected `run` or `_start`): {e}"))?;

run.call_async(&mut store, ()).await?;
```

The function must take no arguments and return nothing. If neither `run` nor `_start` is exported, `run_module` returns an error and the module is not executed.

The call uses Wasmtime's async APIs because some host functions are registered with `func_wrap_async`. Host functions can suspend the module while waiting for runtime work without blocking the tokio thread.

---

## What happens on `abort`

If the module calls `system::abort("message")`, the host function returns a Wasmtime `Error`, which causes an immediate trap. The trap propagates up through `run.call_async`, and `run_module` returns `Err(...)` to the caller. The message appears in the host log.

```rust
// from host_api/system.rs
fn abort_impl(...) -> Result<(), Error> {
    let msg = /* read from guest memory */;
    Err(Error::msg(format!("guest abort: {msg}")))
}
```

The module does not get a chance to run any cleanup code after `abort`. The `Store` is dropped, which drops the `HostState`, which drops the `SyncHandle` clone. In-flight CRDT operations that were already pushed to the sync manager are not rolled back.

---

## Execution is synchronous from the module's perspective

The module calls host functions synchronously. From its perspective, `db::set("key", b"value")` is a blocking call that returns a result code. There are no callbacks, no futures, no async machinery visible to the guest.

The async machinery lives entirely on the host side. When a host function needs to await something (e.g. pushing an op to the sync manager's channel), Wasmtime suspends the fiber running the module, allows the tokio scheduler to run, and resumes the fiber when the operation completes. The module never observes this.

---

## Writing a module: minimal example

```rust
// Cargo.toml
// [lib]
// crate-type = ["cdylib"]
// [dependencies]
// nx-sdk = { path = "../../crates/nx-sdk" }

use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    nx_log!("module started");

    match db::get("counter") {
        Ok(Some(bytes)) => {
            let n = u64::from_le_bytes(bytes.try_into().unwrap_or([0u8; 8]));
            let next = n + 1;
            db::set("counter", &next.to_le_bytes()).unwrap();
            nx_log!("counter = {}", next);
        }
        Ok(None) => {
            db::set("counter", &1u64.to_le_bytes()).unwrap();
            nx_log!("counter = 1");
        }
        Err(e) => nx_log!("error: {}", e),
    }
}
```

Build:

```bash
cargo build --target wasm32-unknown-unknown --release
nx run target/wasm32-unknown-unknown/release/my_module.wasm
```

---

## Related

- [Runtime model](/numax/concepts/runtime-model/) - lifecycle and philosophy
- [Host API](/numax/reference/host-api/) - full function reference
- [nx-sdk crate](/numax/reference/crates/nx-sdk/) - the guest-side SDK
- [nx-core crate](/numax/reference/crates/nx-core/) - Runtime and HostState internals
