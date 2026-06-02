# Distributed Inventory Example

PNCounter-backed inventory replicated across Numax nodes.

This example models a product stock value that can move in both directions:
restocks increase inventory, sales decrease it, and returns increase it again.
Each node applies one local operation, broadcasts it, persists durable CRDT
state, and converges with peers through the sync layer.

## What It Demonstrates

- PNCounter host API: `crdt_pncounter_inc`, `crdt_pncounter_dec`,
  `crdt_pncounter_value`
- SDK wrapper: `nx_sdk::crdt::pncounter`
- signed materialized values with `--print-pncounter`
- two-node and three-node convergence
- durable local state and anti-entropy friendly op-log behavior

## Build

From this example directory:

```bash
cd examples/distributed_inventory
cargo build --release --target wasm32-unknown-unknown
```

The module will be written to:

```text
target/wasm32-unknown-unknown/release/distributed_inventory.wasm
```

## Scenario

All nodes operate on the same key:

```text
inventory:sku-1
```

Supported actions are selected through `NX_INVENTORY_ACTION`:

| Action | Operation | Delta |
|--------|-----------|-------|
| `restock` | increment | `+10` |
| `sale` | decrement | `-4` |
| `return` | increment | `+2` |

If `NX_INVENTORY_ACTION` is not set, the module defaults to `restock`.

## Two Nodes

Run these from the repository root after building the WASM module.

### TOML config alternative

The networking and storage flags can live in config files:

```bash
cat > examples/distributed_inventory/node-a.toml <<'EOF'
[storage]
datastore_path = "examples/distributed_inventory/data-a"

[network]
listen = "127.0.0.1:9101"
peers = ["127.0.0.1:9102"]
serialization_format = "bincode"

[discovery]
mode = "static"
EOF

cat > examples/distributed_inventory/node-b.toml <<'EOF'
[storage]
datastore_path = "examples/distributed_inventory/data-b"

[network]
listen = "127.0.0.1:9102"
peers = ["127.0.0.1:9101"]
serialization_format = "bincode"

[discovery]
mode = "static"
EOF

cargo run -p nx-cli -- config validate --config examples/distributed_inventory/node-a.toml
cargo run -p nx-cli -- config show --config examples/distributed_inventory/node-a.toml --effective
```

Then run the same scenario with shorter commands:

```bash
NX_INVENTORY_ACTION=restock cargo run -p nx-cli -- run \
  examples/distributed_inventory/target/wasm32-unknown-unknown/release/distributed_inventory.wasm \
  --config examples/distributed_inventory/node-a.toml \
  --wait-before-run 1500ms \
  --settle-for 3s \
  --print-pncounter inventory:sku-1 \
  -v
```

```bash
NX_INVENTORY_ACTION=sale cargo run -p nx-cli -- run \
  examples/distributed_inventory/target/wasm32-unknown-unknown/release/distributed_inventory.wasm \
  --config examples/distributed_inventory/node-b.toml \
  --wait-before-run 1500ms \
  --settle-for 3s \
  --print-pncounter inventory:sku-1 \
  -v
```

CLI flags and `NX_*` variables override the file when both are present.

### Node A: Restock

```bash
NX_INVENTORY_ACTION=restock cargo run -p nx -- run \
  examples/distributed_inventory/target/wasm32-unknown-unknown/release/distributed_inventory.wasm \
  --listen 127.0.0.1:9101 \
  --peer 127.0.0.1:9102 \
  --datastore-path examples/distributed_inventory/data-a \
  --wait-before-run 1500ms \
  --settle-for 3s \
  --print-pncounter inventory:sku-1 \
  -v
```

### Node B: Sale

```bash
NX_INVENTORY_ACTION=sale cargo run -p nx -- run \
  examples/distributed_inventory/target/wasm32-unknown-unknown/release/distributed_inventory.wasm \
  --listen 127.0.0.1:9102 \
  --peer 127.0.0.1:9101 \
  --datastore-path examples/distributed_inventory/data-b \
  --wait-before-run 1500ms \
  --settle-for 3s \
  --print-pncounter inventory:sku-1 \
  -v
```

Expected final value on both nodes:

```text
inventory:sku-1 = 6
```

That value comes from:

```text
+10 restock
-4 sale
= 6 available
```

## Three Nodes

Start Node A and Node B as above, then add a return node:

```bash
NX_INVENTORY_ACTION=return cargo run -p nx -- run \
  examples/distributed_inventory/target/wasm32-unknown-unknown/release/distributed_inventory.wasm \
  --listen 127.0.0.1:9103 \
  --peer 127.0.0.1:9101 \
  --peer 127.0.0.1:9102 \
  --datastore-path examples/distributed_inventory/data-c \
  --wait-before-run 1500ms \
  --settle-for 3s \
  --print-pncounter inventory:sku-1 \
  -v
```

Expected final value after all three operations converge:

```text
inventory:sku-1 = 8
```

That value comes from:

```text
+10 restock
-4 sale
+2 return
= 8 available
```

## Reset

Remove the local example datastores before a clean run:

```bash
rm -rf examples/distributed_inventory/data-a \
       examples/distributed_inventory/data-b \
       examples/distributed_inventory/data-c
```

## Notes

- Start the node commands in separate terminals.
- `--wait-before-run` gives peers time to connect before the module emits its
  operation.
- `--settle-for` keeps each runtime alive long enough for PushOps and
  anti-entropy recovery to apply remote operations before printing the final
  value.
- `--print-pncounter inventory:sku-1` reads the host-side materialized value
  after settle, so the result does not depend on guest logs.
- Reusing datastores is intentional for restart/recovery checks. Use the reset
  command when you want a fresh scenario.
- Without `--settle-for`, a sync-enabled runtime stays alive until it receives
  a shutdown signal.
