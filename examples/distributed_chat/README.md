# Local Chat Example

Basic local chat log with Numax's key-value API.

> Note: this example is intentionally not replicated. It uses `nx_sdk::db::*`,
> which writes local datastore entries only. Replicated chat needs a list/set
> CRDT such as ORSet or RGA.

## Build

```bash
cd examples/distributed_chat
cargo build --release --target wasm32-unknown-unknown
```

## Run

```bash
nx run target/wasm32-unknown-unknown/release/distributed_chat.wasm \
    --datastore-path ./chat-data
```

Run multiple times to see messages accumulate!
