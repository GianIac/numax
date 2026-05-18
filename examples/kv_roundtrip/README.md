# KV Roundtrip Example

Raw host API example for Numax's local key-value store.

It calls `db_set`, `db_get`, `db_delete` and `host_log` directly, without using
the SDK.

## Build

```bash
cd examples/kv_roundtrip
cargo build --release --target wasm32-unknown-unknown
```

## Run

```bash
nx run target/wasm32-unknown-unknown/release/kv_roundtrip.wasm \
    --datastore-path ./kv-data
```

You should see the module set `hello = world`, read it back, delete it and then
confirm it is gone.

