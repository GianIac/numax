# Distributed Status Example

LWW-Register-backed service status replicated across Numax nodes.

This example models a single latest-value state, such as service presence,
deployment mode, feature state, or operator-selected status. Each node writes
one local value, broadcasts it, persists durable CRDT state, and converges with
peers through the sync layer.

## What It Demonstrates

- LWW-Register host API: `crdt_lww_set`, `crdt_lww_get`
- SDK wrapper: `nx_sdk::crdt::lww_register`
- host-side convergence output with `--print-lww-register`
- two-node and three-node latest-value convergence
- durable local state and anti-entropy friendly op-log behavior
- connected peer introspection from the guest module

## Build

From this example directory:

```bash
cd examples/distributed_status
cargo build --release --target wasm32-unknown-unknown
```

The module will be written to:

```text
target/wasm32-unknown-unknown/release/distributed_status.wasm
```

## Scenario

All nodes operate on the same key by default:

```text
status:service-a
```

Each node writes the value from `NX_STATUS_VALUE`:

| Env var | Meaning | Default |
|---------|---------|---------|
| `NX_STATUS_KEY` | LWW-Register key | `status:service-a` |
| `NX_STATUS_VALUE` | local status written by this node | `online` |

Useful status values:

```text
online
away
busy
draining
offline
```

## Two Nodes

Run these from the repository root after building the WASM module. Start both
commands in separate terminals. Node B waits slightly longer before running the
module, so its `away` write should be the latest value and win.

### Node A: online

```bash
NX_STATUS_VALUE=online cargo run -p nx -- run \
  examples/distributed_status/target/wasm32-unknown-unknown/release/distributed_status.wasm \
  --listen 127.0.0.1:9201 \
  --peer 127.0.0.1:9202 \
  --datastore-path examples/distributed_status/data-a \
  --wait-before-run 1000ms \
  --settle-for 4s \
  --print-lww-register status:service-a \
  -v
```

### Node B: away

```bash
NX_STATUS_VALUE=away cargo run -p nx -- run \
  examples/distributed_status/target/wasm32-unknown-unknown/release/distributed_status.wasm \
  --listen 127.0.0.1:9202 \
  --peer 127.0.0.1:9201 \
  --datastore-path examples/distributed_status/data-b \
  --wait-before-run 2000ms \
  --settle-for 4s \
  --print-lww-register status:service-a \
  -v
```

Expected final value on both nodes:

```text
status:service-a = away
```

## Three Nodes

Start Node A and Node B as above, then add a third node that writes later:

```bash
NX_STATUS_VALUE=busy cargo run -p nx -- run \
  examples/distributed_status/target/wasm32-unknown-unknown/release/distributed_status.wasm \
  --listen 127.0.0.1:9203 \
  --peer 127.0.0.1:9201 \
  --peer 127.0.0.1:9202 \
  --datastore-path examples/distributed_status/data-c \
  --wait-before-run 3000ms \
  --settle-for 4s \
  --print-lww-register status:service-a \
  -v
```

Expected final value after all three writes converge:

```text
status:service-a = busy
```

## Restart Check

Because LWW-Register state is durable, restarting a node with the same
datastore should hydrate the latest known value before applying a new local
write. For example, rerun Node A with a later status:

```bash
NX_STATUS_VALUE=draining cargo run -p nx -- run \
  examples/distributed_status/target/wasm32-unknown-unknown/release/distributed_status.wasm \
  --listen 127.0.0.1:9201 \
  --peer 127.0.0.1:9202 \
  --datastore-path examples/distributed_status/data-a \
  --wait-before-run 1000ms \
  --settle-for 4s \
  --print-lww-register status:service-a \
  -v
```

Expected final value after it syncs:

```text
status:service-a = draining
```

## Reset

Remove the local example datastores before a clean run:

```bash
rm -rf examples/distributed_status/data-a \
       examples/distributed_status/data-b \
       examples/distributed_status/data-c
```

## Notes

- LWW means "last writer wins": higher host-assigned timestamp wins; equal
  timestamps are resolved deterministically by `NodeId`.
- `--wait-before-run` staggers local writes so the expected winner is
  reproducible when you start terminals around the same time.
- `--settle-for` keeps each runtime alive long enough for PushOps and
  anti-entropy recovery to apply remote operations before printing the final
  value.
- `--print-lww-register status:service-a` reads the host-side materialized
  value after settle, so the result does not depend on guest logs.
- LWW-Register is a good fit for latest-value state. Use PNCounter for numeric
  values that must move up/down, and avoid LWW for collaborative lists or
  inventory quantities.
- Reusing datastores is intentional for restart/recovery checks. Use the reset
  command when you want a fresh scenario.
- Without `--settle-for`, a sync-enabled runtime stays alive until it receives
  a shutdown signal.
