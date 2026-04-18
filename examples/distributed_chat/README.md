# Distributed Chat Example

Basic distributed chat with Numax.

## Build

```bash
cd examples/distributed_chat
cargo build --release --target wasm32-unknown-unknown
```

## Run

```bash
# From the numax root
./target/debug/nx run target/wasm32-unknown-unknown/release/distributed_chat.wasm \
    --sync-prefix "chat:" \
    --datastore-path ./chat-data
```

Run multiple times to see messages accumulate!

## With Sync (two nodes)

Terminal 1:
```bash
./target/debug/nx run target/wasm32-unknown-unknown/release/distributed_chat.wasm \
    --listen 0.0.0.0:9000 \
    --sync-prefix "chat:" \
    --datastore-path ./chat-a
```

Terminal 2:
```bash
./target/debug/nx run target/wasm32-unknown-unknown/release/distributed_chat.wasm \
    --listen 0.0.0.0:9001 \
    --peer 127.0.0.1:9000 \
    --sync-prefix "chat:" \
    --datastore-path ./chat-b
```