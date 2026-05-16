# Hello SDK Example

Smallest Numax SDK example.

It calls `nx_sdk::log`, so the guest talks to the host through the SDK instead
of raw imports.

## Build

```bash
cd examples/hello_sdk
cargo build --release --target wasm32-unknown-unknown
```

## Run

```bash
nx run target/wasm32-unknown-unknown/release/hello_sdk.wasm
```

You should see:

```text
Hello Numax via SDK
```

