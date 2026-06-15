---
title: Writing Host Functions
description: Extend the runtime with new host APIs.
---

This guide explains how to add a new host function to Numax: a function exposed by the runtime to WASM guest modules under the `nx` namespace.

This is an advanced guide for adding a new host API to the runtime and then making it convenient for guest modules through `nx-sdk`. A host function lives in `nx-core`; the SDK only exposes the guest-side wrapper. To follow it, you need a local clone of the repository and changes in both `nx-core` and `nx-sdk`.

The pattern shown here is the most common one in Numax: a function that reads bytes from guest memory, writes bytes into a guest buffer, and returns an `i32` status or byte count.

---

## How host functions work

When a WASM module imports `nx::my_function`, Wasmtime looks for `my_function` in the linker registered for the `nx` namespace. If it finds it, the runtime calls the Rust closure registered there.

For functions that exchange dynamic data, the usual path is:

```
guest: nx_sdk::text::upper(input)
  │
  ├── nx-sdk/src/ffi.rs
  │   unsafe extern "C" { fn string_upper(ptr, len, out_ptr, out_cap) -> i32; }
  │
  ├── WASM linear memory
  │
  └── nx-core/src/host_api/text.rs
      string_upper_impl(caller, ptr, len, out_ptr, out_cap) -> i32
          reads input from guest memory
          does host-side work
          writes output into guest memory
          returns byte count or error code
```

Not every host function has this shape. `time_now()` and `time_monotonic()` return `u64`, legacy `host_log` returns `()`, and `abort` raises a Wasmtime trap. The byte-in/byte-out shape is the right default for APIs that pass strings, lists, or binary payloads.

---

## Step 1 - Choose where it belongs

Host functions are grouped by responsibility in `crates/nx-core/src/host_api/`:

| File | Contains |
|---|---|
| `db.rs` | database operations |
| `crdt.rs` | CRDT operations |
| `system.rs` | env, module id, capabilities, abort, events |
| `time.rs` | `time_now`, `time_monotonic` |
| `crypto.rs` | `random_bytes`, `hash_sha256`, `hash_blake3` |
| `log.rs` | `host_log`, `host_log_v2` |
| `net.rs` | `net_node_id`, `net_peers` |

Add a new file if the function introduces a new responsibility. Add it to an existing file if it naturally belongs there.

For this guide, we add a minimal function: `nx::string_upper`, which reads a UTF-8 string from guest memory and writes back an uppercase version.

---

## Step 2 - Write the host implementation

Create `crates/nx-core/src/host_api/text.rs`:

```rust
use anyhow::Result;
use wasmtime::{Caller, Linker, Memory};

use crate::runtime::HostState;

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const MAX_INPUT_LEN: u32 = 64 * 1024;
const MAX_OUT_CAP: u32 = 1024 * 1024;

fn get_memory(caller: &mut Caller<'_, HostState>) -> Option<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(mem)) => Some(mem),
        _ => None,
    }
}

fn string_upper_impl(
    mut caller: Caller<'_, HostState>,
    in_ptr: u32,
    in_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    let memory = match get_memory(&mut caller) {
        Some(memory) => memory,
        None => {
            eprintln!("[nx-core] string_upper: no `memory` export on guest");
            return ERR_INTERNAL;
        }
    };

    if in_len > MAX_INPUT_LEN {
        eprintln!("[nx-core] string_upper: input too large: {in_len}");
        return ERR_INTERNAL;
    }

    if out_cap > MAX_OUT_CAP {
        eprintln!("[nx-core] string_upper: output cap too large: {out_cap}");
        return ERR_INTERNAL;
    }

    let mut input = vec![0u8; in_len as usize];
    if let Err(e) = memory.read(&mut caller, in_ptr as usize, &mut input) {
        eprintln!("[nx-core] string_upper: failed to read input: {e}");
        return ERR_INTERNAL;
    }

    let input = match std::str::from_utf8(&input) {
        Ok(input) => input,
        Err(e) => {
            eprintln!("[nx-core] string_upper: input is not UTF-8: {e}");
            return ERR_INTERNAL;
        }
    };

    let result = input.to_uppercase();
    let result = result.as_bytes();

    if result.len() > out_cap as usize {
        return ERR_BUF_TOO_SMALL;
    }

    if let Err(e) = memory.write(&mut caller, out_ptr as usize, result) {
        eprintln!("[nx-core] string_upper: failed to write output: {e}");
        return ERR_INTERNAL;
    }

    result.len() as i32
}

pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap(
        "nx",
        "string_upper",
        |caller: Caller<'_, HostState>,
         in_ptr: u32,
         in_len: u32,
         out_ptr: u32,
         out_cap: u32|
         -> i32 { string_upper_impl(caller, in_ptr, in_len, out_ptr, out_cap) },
    )?;

    Ok(())
}
```

The important points are:

- get the `memory` export from the guest;
- validate every length before reading;
- read from guest memory with `memory.read`;
- write into guest memory only if `out_cap` is large enough;
- return `ERR_BUF_TOO_SMALL` when the SDK can retry with a larger buffer;
- avoid `panic!` in error paths.

---

## Step 3 - Register the host API module

Add the new module to `crates/nx-core/src/host_api/mod.rs`:

```rust
pub mod crdt;
pub mod crypto;
pub mod db;
pub mod log;
pub mod net;
pub mod system;
pub mod text;
pub mod time;
```

---

## Step 4 - Link it into the runtime

In `crates/nx-core/src/runtime.rs`, in the block where the runtime registers the other host APIs, add:

```rust
host_api::text::add_to_linker(&mut linker)?;
```

Every function exposed to the guest must be registered here, or through an `add_to_linker` called from here.

---

## Step 5 - Add the capability

In `crates/nx-core/src/host_api/system.rs`, add `"string_upper"` to `HOST_CAPABILITIES`.

This lets guest modules call `system::host_capabilities()` and discover that the function is available in the runtime.

---

## Step 6 - Add the SDK FFI import

In `crates/nx-sdk/src/ffi.rs`, add the function inside the existing import block:

```rust
#[link(wasm_import_module = "nx")]
unsafe extern "C" {
    // ... existing imports ...

    pub fn string_upper(
        in_ptr: u32,
        in_len: u32,
        out_ptr: u32,
        out_cap: u32,
    ) -> i32;
}
```

The name must exactly match the function registered in the linker: `string_upper`.

---

## Step 7 - Add the SDK wrapper

Create `crates/nx-sdk/src/text.rs`:

```rust
use crate::__alloc::{string::String, vec};
use crate::{ffi, NxError, Result};

const ERR_BUF_TOO_SMALL: i32 = -2;
const ERR_INTERNAL: i32 = -3;
const MAX_RETRY_CAP: usize = 1024 * 1024;

/// Converts a UTF-8 string to uppercase using the Numax host runtime.
pub fn upper(input: &str) -> Result<String> {
    let input = input.as_bytes();
    let mut cap = input.len().saturating_add(64).max(64);

    loop {
        let mut out = vec![0u8; cap];
        let rc = unsafe {
            ffi::string_upper(
                input.as_ptr() as u32,
                input.len() as u32,
                out.as_mut_ptr() as u32,
                out.len() as u32,
            )
        };

        match rc {
            n if n >= 0 => {
                out.truncate(n as usize);
                return String::from_utf8(out).map_err(|_| NxError::Internal);
            }
            ERR_BUF_TOO_SMALL => {
                cap = cap.saturating_mul(2);
                if cap > MAX_RETRY_CAP {
                    return Err(NxError::BufferTooSmall);
                }
            }
            ERR_INTERNAL => return Err(NxError::Internal),
            code => return Err(NxError::UnknownCode(code)),
        }
    }
}
```

Register the module in `crates/nx-sdk/src/lib.rs`:

```rust
pub mod text;
```

---

## Step 8 - Use it from a guest module

```rust
use nx_sdk::{nx_log, text};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    match text::upper("hello numax") {
        Ok(value) => nx_log!("upper: {}", value),
        Err(e) => nx_log!("error: {}", e),
    }
}
```

Expected output:

```text
upper: HELLO NUMAX
```

---

## Practical ABI rules

For byte-in/byte-out host functions, use these conventions:

**Dynamic inputs** as `(ptr: u32, len: u32)` pairs. The host reads `len` bytes from guest linear memory at offset `ptr`.

**Dynamic outputs** as `(out_ptr: u32, out_cap: u32)`. The host writes at most `out_cap` bytes into guest memory. If the result does not fit, it returns `ERR_BUF_TOO_SMALL` (`-2`) and the SDK retries with a larger buffer.

**Return value** usually `i32`:

| Code | Meaning |
|---|---|
| `>= 0` | success; for dynamic outputs this is the number of bytes written |
| `-1` | not found, mostly used by `db_get` |
| `-2` | output buffer too small |
| `-3` | internal error |
| `-4` | reserved `__nx/` key |
| `-5` | sync disabled |

This is a convention, not a universal law. Some APIs have simpler signatures because they do not pass dynamic buffers.

---

## Async host functions

If the function needs to `await`, use `func_wrap_async`.

Minimal example:

```rust
pub fn add_to_linker(linker: &mut Linker<HostState>) -> Result<()> {
    linker.func_wrap_async(
        "nx",
        "my_async_function",
        |caller: Caller<'_, HostState>,
         (in_ptr, in_len, out_ptr, out_cap): (u32, u32, u32, u32)| {
            Box::new(my_async_function_impl(
                caller,
                in_ptr,
                in_len,
                out_ptr,
                out_cap,
            ))
        },
    )?;

    Ok(())
}

async fn my_async_function_impl(
    mut caller: Caller<'_, HostState>,
    in_ptr: u32,
    in_len: u32,
    out_ptr: u32,
    out_cap: u32,
) -> i32 {
    // can use .await here
    0
}
```

CRDT functions and some network functions use this pattern because they interact with shared state or async channels.

---

## What not to do

**Do not access global host resources by bypassing `HostState`.** `HostState` is where the runtime attaches the store, sync handle, metrics, and invocation configuration.

**Do not write more than `out_cap` bytes.** Even if guest memory contains valid space after that buffer, the ABI contract says the host may only write into the region declared by the guest.

**Do not use `std::process::exit`.** A host function must not terminate the runtime process. If it needs to stop the guest, return an error or use an explicit Wasmtime trap, like `abort` does.

**Do not register the same name twice.** Wasmtime returns an error when building the linker.

---

## Related

- [WASM execution](/numax/concepts/wasm-execution/) - how the linker and `HostState` work
- [Host API reference](/numax/reference/host-api/) - the host functions exposed by the runtime
- [nx-core crate](/numax/reference/crates/nx-core/) - runtime and host API internals
- [nx-sdk crate](/numax/reference/crates/nx-sdk/) - guest-side wrappers over raw imports
