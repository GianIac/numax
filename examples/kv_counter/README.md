# KV Counter Example

Persistent local counter using Numax's key-value host API through the SDK.

This example is intentionally local-only: it uses `nx_sdk::db::*`, not CRDT
replication.

## Build

```bash
cd examples/kv_counter
cargo build --release --target wasm32-unknown-unknown
```

## Run

```bash
nx run target/wasm32-unknown-unknown/release/kv_counter.wasm \
    --datastore-path ./counter-data
```

Run it more than once with the same datastore. The printed value should grow by
one each time.

