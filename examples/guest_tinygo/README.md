# Guest Example in TinyGo

Basic TinyGo guest example for Numax which compiles to `.wasm` and performs logging and simple key/value storage.

> Note: This example intentionally keeps the guest minimal and avoids heavy standard library features to make the ABI interaction easier to inspect and reason about.

## TinyGo ABI Note

This example relies on compiler directives to explicitly control the generated WebAssembly symbol names and imports.

Instead of traditional C-style header inclusions, TinyGo uses:

``` bash
//go:wasmimport <module> <name>
//go:export <name>
```

to explicitly control the generated WebAssembly symbol names and imports.

### Wasm binary

on inspection of the `.wasm` binary, the exported and imported WebAssembly ABI symbols remain clean because of using explicit directives, mapping directly to the nx namespace (no name mangling)

```wasm
  (import "nx" "host_log_v2" (func $main.nxHostLogV2 (type $t0)))
  (import "nx" "db_set" (func $main.nxDbSet (type $t1)))
  (func $main.run (export "run") (type $t2)
```

> Note: Even though Go uses packages and module paths internally, the exported/imported WASM ABI remains clean and stable using //go:wasmimport and //go:export.

## Requirements

- `TinyGo` installed and in your PATH.
- A built nx runtime.
- `wasm-opt` installed via a method of your choice (tested with `npm i -g wasm-opt`)

## Build

The runtime can be built from the repository root with:

```bash
cargo build --release
```

### Windows

```bash
cd /examples/guest_tinygo
./build.bat
```

### Linux / macOS

```bash
cd /examples/guest_tinygo
chmod +x build.sh
./build.sh
```

This generates:

```bash
guest.wasm
```

In the `guest_tinygo` directory

> Note: the scripts use the -opt=0 and the -no-debug arguments since TinyGo's default wasm target injects browser-oriented runtime hooks and missing imports in some cases 
>
> [nx-cli] error: Link error while instantiating module: unknown import: `gojs::runtime.ticks` has not been defined

## Run

from the repository root...

### Run on Windows

```bash
./target/release/nx.exe run ./examples/guest_tinygo/guest.wasm
```

### Run on macOS/Linux

```bash
./target/release/nx run ./examples/guest_tinygo/guest.wasm
```

## Output

```bash
[guest] Hello from TinyGo guest!
[guest] db_set ok
```
