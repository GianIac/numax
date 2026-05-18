# Guest example in C++

Basic C++ example which compiles to WASM and performs a simple logging + DB-write style example

> Note: This example intentionally keeps the guest minimal and avoids libc / stdlib usage to make the ABI interaction easier to inspect and reason about.

## C++ ABI Note

This example intentionally does not use `extern "C"`.

Instead, the guest relies on:

- `import_name(...)`
- `import_module(...)`
- `export_name(...)`

to explicitly control the generated WebAssembly symbol names.

### Note

on inspection of the .wasm binary,  the **internal** compiler-generated symbol names are transformed by the C++ compilation process
while the **exported** WebAssembly ABI symbols remain stable due to the explicit `export_name(...)` attribute as seen below

```wasm
  (import "nx" "host_log_v2" (func $host_log_v2_char_const*__int_ (type $t0)))
  (import "nx" "db_set" (func $db_set_char_const*__int__char_const*__int_ (type $t1)))
  (func $run__ (export "run") (type $t2)
```

here the exported function "run" is actually internally compiled as "run__" however on exporting to WebAssembly ABI it exports "run" once again which is an interesting note.

> Note: Even though C++ internally mangles symbol names, the exported/imported WASM ABI remains stable.
> This is because the WebAssembly-facing symbols are explicitly defined through attributes (really interesting quirk of the import abi)

## Important

- Host functions are imported manually through the `nx` namespace.
- Strings are passed as raw pointers + lengths.
- The example uses `-nostdlib` for a minimal WASM module.
- `--allow-undefined` is required because Numax provides imports at runtime.

hence after the .wasm binary inspection, it means that explicit WASM import/export attributes **can** stabilize ABI names even without `extern "C"`

## Requirements

- WASI SDK / clang in path
- A built `nx` runtime

The runtime can be built from the repository root with:

```bash
cargo build --release
```

## Build

### Windows

```bash
cd /examples/guest_cpp
./build.bat
```

### Linux / macOS

```bash
cd /examples/guest_cpp
chmod +x build.sh
./build.sh
```

This generates:

```bash
guest.wasm
```

In the guest_cpp directory

## Run

From the repository root:

### Run on Windows

```bash
.\target\release\nx.exe run .\examples\guest_cpp\guest.wasm
```

### Run on MacOS/Linux

```bash
./target/release/nx run ./examples/guest_cpp/guest.wasm
```

## output

```bash
[guest] Hello from C++ guest!
[guest] db_set ok
```
