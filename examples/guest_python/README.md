# Guest Example in Python

Minimal Python guest example for Numax, using RustPython embedded in a tiny Rust wrapper and compiled to a core WebAssembly module.

## Why not componentize-py

`componentize-py` produces a WebAssembly *Component*. The current Numax runtime loads guests as core WebAssembly modules via `wasmtime::Module`,  which rejects Components (see #66, #57, #61). This example therefore uses RustPython compiled to `wasm32-wasip1`

> the same core-module target is already used by the TinyGo and C/C++ examples.

## ABI Note

- `guest.py` is embedded into the binary at compile time (`include_str!`)
  — no filesystem access needed at runtime.
- A native `nx` module is registered on RustPython's `InterpreterBuilder`
  before the interpreter is built, exposing `nx.log` and `nx.db_set` to
  Python via `import nx`. Those calls are backed by raw `extern "C"`
  imports from the `nx` namespace: `host_log_v2` and `db_set`.
- No standard library is registered on the builder — only Python
  builtins are available (`import os`, `import sys`, etc. won't work)
  — this avoids needing to freeze/embed the stdlib into the WASM binary.
- The compiled module exports a single `run` function, matching the
  C/C++/TinyGo guests.

## Requirements

- Rust + Cargo, with the `wasm32-wasip1` target (`rustup target add wasm32-wasip1`)
- A built `nx` runtime (`cargo build --release` from the repo root)

## Build

From the repository root:

### Windows

```bash
cd examples/guest_python
./build.bat
```

### Linux / macOS

```bash
cd examples/guest_python
chmod +x build.sh
./build.sh
```

Produces `guest.wasm` in the `examples/guest_python` directory

## Run

From the repository root

### Run on Windows

```bash
./target/release/nx.exe run ./examples/guest_python/guest.wasm
```

### Run on Linux / macOS

```bash
./target/release/nx run ./examples/guest_python/guest.wasm
```

## Output

```bash
[guest] Hello from Python guest!
[guest] db_set ok
```

## Details

- Python: 3.12 semantics via RustPython (not CPython)
- RustPython: `rustpython-vm = "0.5"` from crates.io
- WASM target: `wasm32-wasip1`
- Tested on: Windows

## Limitations

- RustPython is not CPython — some language/library edge cases differ.
- No standard library is embedded; this guest is intentionally
  builtins-only.
- `componentize-py` is not used, per the current runtime's core-module
  requirement (see above). Once Component Model support lands, a
  sibling `guest_python_component` example could use it

- Unlike the C/C++ guests, which pass fixed-size byte buffers, this guest
  encodes Python `str` values as UTF-8 before crossing the WASM boundary.
  For ASCII content the byte length matches the character count, as here,
  but a Python guest passing non-ASCII text would need callers to be aware
  lengths are byte lengths, not character counts
