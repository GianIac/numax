# Guest example in C

Basic C example which compiles to WASM and performs a simple logging + DB-write style example

> Note: This example intentionally keeps the guest minimal and avoids libc / stdlib usage to make the ABI interaction easier to inspect and reason about.

## Important

- Host functions are imported manually through the `nx` namespace.
- Strings are passed as raw pointers + lengths.
- The example uses `-nostdlib` for a minimal WASM module.
- `--allow-undefined` is required because Numax provides imports at runtime.

## Requirements

- WASI SDK or clang with WebAssembly target support and `wasm-ld` in PATH
- Node.js 22 for the ABI smoke test
- A built `nx` runtime (only needed to run the guest in Numax)

The runtime can be built from the repository root with:

```bash
cargo build --release
```

## Build

From the repository root:

### Windows

```bash
cd examples/guest_c
./build.bat
```

### Linux / macOS

```bash
cd examples/guest_c
chmod +x build.sh
./build.sh
```

This generates:

```bash
guest.wasm
```

The output is written to the `examples/guest_c` directory.

### ABI smoke test

After building `guest.wasm`, run from `examples/guest_c`:

```bash
node test.mjs
```

Alternatively, run from the repository root:

```bash
node examples/guest_c/test.mjs
```

The smoke test checks the `nx.host_log_v2` and `nx.db_set` imports, the
`memory` and `run` exports, the log messages, and the key/value pair written by
the guest. It uses mock host functions and does not require a running Numax
instance.

## Run

From the repository root:

### Run on Windows

```bash
.\target\release\nx.exe run .\examples\guest_c\guest.wasm
```

### Run on Linux / macOS

```bash
./target/release/nx run ./examples/guest_c/guest.wasm
```

Output:

```bash
[guest] Hello from C guest!
[guest] db_set ok
```
