# Distributed Chat Example

Chat distribuita basilare con Numax.

## Build

```bash
cd examples/distributed_chat
cargo build --release --target wasm32-unknown-unknown
```

## Run

```bash
# Dalla root di numax
./target/debug/nx run target/wasm32-unknown-unknown/release/distributed_chat.wasm \
    --sync-prefix "chat:" \
    --datastore-path ./chat-data
```

Esegui più volte per vedere i messaggi accumularsi!

## Con Sync (due nodi)

Terminale 1:
```bash
./target/debug/nx run target/wasm32-unknown-unknown/release/distributed_chat.wasm \
    --listen 0.0.0.0:9000 \
    --sync-prefix "chat:" \
    --datastore-path ./chat-a
```

Terminale 2:
```bash
./target/debug/nx run target/wasm32-unknown-unknown/release/distributed_chat.wasm \
    --listen 0.0.0.0:9001 \
    --peer 127.0.0.1:9000 \
    --sync-prefix "chat:" \
    --datastore-path ./chat-b
```