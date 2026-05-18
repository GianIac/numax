# Hello WASM Example

Minimal raw WebAssembly guest for Numax.

It imports `nx.host_log` directly and exposes a `run` entrypoint.

## Build

```bash
cd examples/hello_wasm
cargo build --release --target wasm32-unknown-unknown
```

## Run

```bash
nx run target/wasm32-unknown-unknown/release/hello_wasm.wasm
```

You should see:

```text
Hello from WASM by NumaX !!
```

