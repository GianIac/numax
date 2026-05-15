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
    --peer 127.0.0.1:9001 \
    --datastore-path ./data-a \
    --wait-before-run 1500ms \
    --settle-for 2s \
    --print-gcounter counter:visits \
    -v
```

### Node B (connects to A)

```bash
nx run target/wasm32-unknown-unknown/release/distributed_counter.wasm \
    --listen 0.0.0.0:9001 \
    --peer 127.0.0.1:9000 \
    --datastore-path ./data-b \
    --wait-before-run 1500ms \
    --settle-for 2s \
    --print-gcounter counter:visits \
    -v
```

## Expected behavior

Each invocation runs the guest once, waits for the configured settle window,
prints the final materialized GCounter value, flushes the local store and exits:
- each run performs one local increment (`crdt::gcounter::inc(key, 1)`)
- the increment is broadcast to peers asynchronously
- `--wait-before-run` gives both processes time to connect before the guest
  emits its increment
- `--settle-for` gives PushOps and remote apply time to complete before exit
- `--print-gcounter counter:visits` prints a final host-side value after settle

With the two commands above, both nodes should print:

```text
counter:visits = 2
```

## Notes

- No `--sync-prefix` flag exists anymore: replication is driven by the API
  surface (`nx_sdk::crdt::*`), not by key prefix. Everything written via
  `nx_sdk::db::*` is purely local.
- Sync requires `--listen <addr>`. Running the guest without `--listen`
  will log a message and exit, since `crdt::*` APIs require replication
  to be enabled on the runtime.
- Each node has its own datastore (`./data-a`, `./data-b`). Counter totals are
  materialized to sled after local and remote updates, flushed on shutdown and
  hydrated back into the in-memory registry on startup.
- Without `--settle-for`, a sync-enabled runtime stays alive until it receives
  SIGINT/SIGTERM/SIGHUP.
- The `-v` flag enables verbose logs to observe lifecycle, broadcast and apply
  paths.
