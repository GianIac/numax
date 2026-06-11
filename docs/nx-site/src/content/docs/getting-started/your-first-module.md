---
title: Your First Module
description: Write and run a minimal WASM module.
---

## What is a Numax module?

A Numax module is a `.wasm` file with one exported function called `run`.
When you execute `nx run your_module.wasm`, the runtime loads the file,
links the host API, and calls `run()` once. Storage, networking, sync - all optional, all on your terms.

Any language that compiles to WASM can be a Numax module. This page shows Rust
(with and without the SDK), C, C++, and a preview of what Go and Python will look like.

---

## What you need

Numax already built from the [Quickstart](/getting-started/quickstart-5-min/).
If not:

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo build --release
export NX=./target/release/nx
```

---

## Rust - with nx-sdk

The recommended way. The SDK wraps all raw host imports into normal Rust functions.

### Step 1 - Create the crate

```bash
cargo new --lib my_module
cd my_module
```

Open `Cargo.toml` and replace its content with:

```toml
[package]
name = "my_module"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
nx-sdk = { path = "../crates/nx-sdk" }

[profile.release]
lto = true
opt-level = "z"
codegen-units = 1
panic = "abort"

[workspace]
```

Two things to note:

- `crate-type = ["cdylib"]` - tells Rust to produce a `.wasm` file instead of a normal Rust library.
  Without this, `cargo build` produces a `.rlib` that Numax cannot load.
- `nx-sdk = { path = "../crates/nx-sdk" }` - the guest SDK.
  It wraps the raw host imports (`db_get`, `host_log_v2`, `gcounter_inc`, ...) into normal
  Rust functions so you never have to touch FFI directly.

### Step 2 - Write the module

Open `src/lib.rs` and replace everything with:

```rust
use nx_sdk::log;

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    log("Hello from my first Numax module!");
}
```

Line by line:

- `use nx_sdk::log` - imports the `log` function from the SDK. Under the hood it calls
  `host_log_v2` on the Numax host, which prints to the terminal.
- `#[unsafe(no_mangle)]` - tells the Rust compiler to keep the function name as-is in the
  compiled `.wasm`. Without it, Rust mangles the name (e.g. `_ZN9my_module3runE`) and
  Numax cannot find the entry point.
- `pub extern "C"` - exports the function with the C ABI so WASM can expose it correctly.
- `run()` - the name Numax looks for. You can put whatever logic you want inside.

### Step 3 - Build and run

```bash
cargo build --release --target wasm32-unknown-unknown
cd ..
export WASM=my_module/target/wasm32-unknown-unknown/release/my_module.wasm
$NX run $WASM
```

`wasm32-unknown-unknown` is the Rust target for bare WASM - no OS, no WASI, just a `.wasm`
binary. Numax provides the host functions itself at runtime.

Output:

```text
[guest] Hello from my first Numax module!
```

The `[guest]` prefix is added by the host to distinguish module logs from runtime logs.

---

## Rust - formatted logs and local storage

### Formatted logs

The `log()` function takes a plain `&str`. For formatted output use `nx_log!` - it works
exactly like `println!` but routes through the host:

```rust
use nx_sdk::{log, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    log("Module started.");
    nx_log!("1 + 1 = {}", 1 + 1);
}
```

### Local storage

Read and write persistent data with `nx_sdk::db`. Data written here lives in the node's
local datastore (sled on disk) and is **not replicated** - every node has its own copy.

```rust
use nx_sdk::{db, nx_log};

#[unsafe(no_mangle)]
pub extern "C" fn run() {
    // Write a value
    db::set("my_key", b"hello numax").unwrap();

    // Read it back
    match db::get("my_key") {
        Ok(Some(bytes)) => nx_log!("Read: {}", String::from_utf8_lossy(&bytes)),
        Ok(None)        => nx_log!("Key not found."),
        Err(e)          => nx_log!("Error: {:?}", e),
    }

    // Delete it
    db::delete("my_key").unwrap();
}
```

The datastore persists between runs. For a clean slate, delete the datastore directory
(default: `./nx-data`). For data that must be consistent across nodes, use
`nx_sdk::crdt::*` instead of `db::*`.

Full example: [`hello_sdk`](https://github.com/GianIac/numax/tree/main/examples/hello_sdk)

---

## C

No SDK needed. You import the host functions manually through the `nx` namespace
using `__attribute__((import_module))` and `__attribute__((import_name))`.
Strings are passed as raw pointer + length pairs.

You need `clang` with WASM target support. On macOS: `brew install llvm`. On Linux: `apt install clang`.

```c
__attribute__((import_module("nx")))
__attribute__((import_name("host_log_v2")))
extern int host_log_v2(const char* ptr, int len);

__attribute__((import_module("nx")))
__attribute__((import_name("db_set")))
extern int db_set(
    const char* key_ptr, int key_len,
    const char* val_ptr, int val_len
);

__attribute__((export_name("run")))
void run() {
    const char msg[] = "Hello from C guest!";
    host_log_v2(msg, sizeof(msg) - 1);

    const char* key = "hello";
    const char* val = "numax";
    db_set(key, 5, val, 5);

    const char done[] = "db_set ok";
    host_log_v2(done, sizeof(done) - 1);
}
```

Build:

```bash
clang \
  --target=wasm32-wasip1 \
  -O3 \
  -nostdlib \
  -Wl,--no-entry \
  -Wl,--export=run \
  -Wl,--allow-undefined \
  -o guest.wasm \
  src/guest.c
```

Run:

```bash
$NX run ./guest.wasm
```

Output:

```text
[guest] Hello from C guest!
[guest] db_set ok
```

Full example: [`guest_c`](https://github.com/GianIac/numax/tree/main/examples/guest_c)

---

## C++

Same as C but C++ mangles function names internally. Use `export_name("run")` to keep
the exported WASM symbol stable regardless of what the compiler does internally.

```cpp
__attribute__((import_module("nx")))
__attribute__((import_name("host_log_v2")))
extern int host_log_v2(const char* ptr, int len);

__attribute__((import_module("nx")))
__attribute__((import_name("db_set")))
extern int db_set(
    const char* key_ptr, int key_len,
    const char* val_ptr, int val_len
);

__attribute__((export_name("run")))
void run() {
    const char msg[] = "Hello from C++ guest!";
    host_log_v2(msg, sizeof(msg) - 1);

    const char key[] = "hello";
    const char val[] = "numax-cpp";
    db_set(key, sizeof(key) - 1, val, sizeof(val) - 1);

    const char done[] = "db_set ok";
    host_log_v2(done, sizeof(done) - 1);
}
```

Build:

```bash
clang++ \
  --target=wasm32-wasip1 \
  -O3 \
  -nostdlib \
  -Wl,--no-entry \
  -Wl,--export=run \
  -Wl,--allow-undefined \
  -o guest.wasm \
  src/guest.cpp
```

Output:

```text
[guest] Hello from C++ guest!
[guest] db_set ok
```

Full example: [`guest_cpp`](https://github.com/GianIac/numax/tree/main/examples/guest_cpp)

---

## Go (preview)

Go can compile to WASM via `GOARCH=wasm GOOS=wasip1`. The same contract applies:
export a function called `run`, import host functions from the `nx` namespace.

```go
//go:build wasm

package main

import "unsafe"

//go:wasmimport nx host_log_v2
func hostLog(ptr *byte, len int32) int32

func logStr(s string) {
    b := []byte(s)
    hostLog(&b[0], int32(len(b)))
}

//go:export run
func run() {
    logStr("Hello from Go guest!")
}

func main() {}
```

Build:

```bash
GOARCH=wasm GOOS=wasip1 go build -o guest.wasm main.go
$NX run ./guest.wasm
```

> Full Go example coming soon.

---

## Python (preview)

Python can target WASM via [Extism](https://extism.org/) PDK or by compiling with
[py2wasm](https://wasmer.io/posts/py2wasm-a-python-to-wasm-compiler). The host contract
is the same: one exported `run` function, host imports from the `nx` namespace.

```python
# Conceptual - full example coming soon

def run():
    log("Hello from Python guest!")
```

> Full Python example coming soon.

---

## More examples coming

Go, Python, AssemblyScript, Zig - if it compiles to WASM, it runs on Numax.
More examples are on the way.

In the meantime, browse everything already available in the
[examples directory](https://github.com/GianIac/numax/tree/main/examples).

---

## Rebuild after every change

```bash
# Rust
cargo build --release --target wasm32-unknown-unknown

# C / C++
./build.sh  # or build.bat on Windows

$NX run $WASM
```

---

## More examples coming

Go, Python, AssemblyScript, Zig - if it compiles to WASM, it runs on Numax.
More examples are on the way.

If you want to request an example or contribute one directly, open an issue or a PR -
it's more than welcome.

In the meantime, browse everything already available in the
[examples directory](https://github.com/GianIac/numax/tree/main/examples).

---

## Next steps

- Make it distributed - [Quickstart: 5 Minutes](/getting-started/quickstart-5-min/)
- Explore the full SDK: `nx_sdk::crdt`, `nx_sdk::net`, `nx_sdk::system`, `nx_sdk::time`
- Browse the [examples directory](https://github.com/GianIac/numax/tree/main/examples)