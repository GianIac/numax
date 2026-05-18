# Hello WASI Example

Minimal WASI module running on Numax.

It uses standard output and reads the WASI argument list provided by the host.

## Build

```bash
cd examples/hello_wasi
cargo build --release --target wasm32-wasip1
```

## Run

```bash
nx run target/wasm32-wasip1/release/hello_wasi.wasm
```

You should see a hello message and the arguments visible to the WASI guest.

