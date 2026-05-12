# Distributed Counter Example

Grow-only distributed counter using Numax's CRDT replication (GCounter).

## Build

```bash
cd examples/distributed_counter
cargo build --release --target wasm32-unknown-unknown
```

## Run

### Node A (first node)

```bash
nx run target/wasm32-unknown-unknown/release/distributed_counter.wasm \
    --listen 0.0.0.0:9000 \
    --datastore-path ./data-a \
    -v
```

### Node B (connects to A)

```bash
nx run target/wasm32-unknown-unknown/release/distributed_counter.wasm \
    --listen 0.0.0.0:9001 \
    --peer 127.0.0.1:9000 \
    --datastore-path ./data-b \
    -v
```

## Expected behavior

Running the guest repeatedly on both nodes, the counter converges:
- each run performs one local increment (`crdt::gcounter::inc(key, 1)`)
- the increment is broadcast to peers asynchronously
- subsequent runs on any node observe the sum of all increments produced
  across the cluster so far

Example: run 3 times on A, 2 times on B → after replication settles, both
nodes report `value = 5`.

## Notes

- No `--sync-prefix` flag exists anymore: replication is driven by the API
  surface (`nx_sdk::crdt::*`), not by key prefix. Everything written via
  `nx_sdk::db::*` is purely local.
- Sync requires `--listen <addr>`. Running the guest without `--listen`
  will log a message and exit, since `crdt::*` APIs require replication
  to be enabled on the runtime.
- Each node has its own datastore (`./data-a`, `./data-b`). Counter totals are
  materialized to sled after local and remote updates; startup recovery from
  those materialized values is still planned work.
- The `-v` flag enables verbose logs to observe the broadcast / apply path.
