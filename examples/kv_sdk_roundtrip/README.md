# KV SDK Roundtrip Example

SDK version of the local key-value roundtrip.

It uses `nx_sdk::db` to set, read and delete one local key.

## Build

```bash
cd examples/kv_sdk_roundtrip
cargo build --release --target wasm32-unknown-unknown
```

## Run

```bash
nx run target/wasm32-unknown-unknown/release/kv_sdk_roundtrip.wasm \
    --datastore-path ./kv-data
```

You should see the value read back and then removed.

