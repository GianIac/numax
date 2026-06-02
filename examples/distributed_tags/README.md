# Distributed Tags Example

ORSet-backed tags replicated across Numax nodes.

This example models a distributed tag set for one document. Nodes can add tags,
remove tags they have observed, list visible tags, persist durable ORSet state,
and converge with peers through the sync layer.

## What It Demonstrates

- ORSet host API: `crdt_orset_add`, `crdt_orset_remove`,
  `crdt_orset_contains`, `crdt_orset_elements`
- SDK wrapper: `nx_sdk::crdt::orset`
- host-side convergence output with `--print-orset`
- concurrent add convergence
- observed-remove semantics
- durable local state and anti-entropy friendly op-log behavior

## Build

From this example directory:

```bash
cd examples/distributed_tags
cargo build --release --target wasm32-unknown-unknown
```

The module will be written to:

```text
target/wasm32-unknown-unknown/release/distributed_tags.wasm
```

## Scenario

All nodes operate on the same key by default:

```text
tags:doc-1
```

Each run is configured through environment variables:

| Env var | Meaning | Default |
|---------|---------|---------|
| `NX_TAG_KEY` | ORSet key | `tags:doc-1` |
| `NX_TAG_ACTION` | `add`, `remove`, or `list` | `add` |
| `NX_TAG_VALUE` | tag affected by `add` or `remove` | `urgent` |

Useful tags:

```text
urgent
review
backend
frontend
blocked
```

## Two Nodes: Concurrent Adds

Run these from the repository root after building the WASM module. Start both
commands in separate terminals.

### TOML config alternative

Use one file per node for stable network/storage settings:

```bash
cat > examples/distributed_tags/node-a.toml <<'EOF'
[storage]
datastore_path = "examples/distributed_tags/data-a"

[network]
listen = "127.0.0.1:9301"
peers = ["127.0.0.1:9302"]
serialization_format = "bincode"

[discovery]
mode = "static"
EOF

cat > examples/distributed_tags/node-b.toml <<'EOF'
[storage]
datastore_path = "examples/distributed_tags/data-b"

[network]
listen = "127.0.0.1:9302"
peers = ["127.0.0.1:9301"]
serialization_format = "bincode"

[discovery]
mode = "static"
EOF

cargo run -p nx-cli -- config validate --config examples/distributed_tags/node-a.toml
```

Then run the add scenario with shorter commands:

```bash
NX_TAG_ACTION=add NX_TAG_VALUE=urgent cargo run -p nx-cli -- run \
  examples/distributed_tags/target/wasm32-unknown-unknown/release/distributed_tags.wasm \
  --config examples/distributed_tags/node-a.toml \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-orset tags:doc-1 \
  -v
```

```bash
NX_TAG_ACTION=add NX_TAG_VALUE=review cargo run -p nx-cli -- run \
  examples/distributed_tags/target/wasm32-unknown-unknown/release/distributed_tags.wasm \
  --config examples/distributed_tags/node-b.toml \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-orset tags:doc-1 \
  -v
```

CLI flags and `NX_*` variables override the file when both are present.

### Node A: add urgent

```bash
NX_TAG_ACTION=add NX_TAG_VALUE=urgent cargo run -p nx-cli -- run \
  examples/distributed_tags/target/wasm32-unknown-unknown/release/distributed_tags.wasm \
  --listen 127.0.0.1:9301 \
  --peer 127.0.0.1:9302 \
  --datastore-path examples/distributed_tags/data-a \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-orset tags:doc-1 \
  -v
```

### Node B: add review

```bash
NX_TAG_ACTION=add NX_TAG_VALUE=review cargo run -p nx-cli -- run \
  examples/distributed_tags/target/wasm32-unknown-unknown/release/distributed_tags.wasm \
  --listen 127.0.0.1:9302 \
  --peer 127.0.0.1:9301 \
  --datastore-path examples/distributed_tags/data-b \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-orset tags:doc-1 \
  -v
```

Expected final value on both nodes:

```text
tags:doc-1 = [review, urgent]
```

ORSet keeps both concurrent adds because each add has its own unique tag.

## Observed Remove

After the two-node add scenario has converged, remove `urgent` from Node A using
the same datastore:

```bash
NX_TAG_ACTION=remove NX_TAG_VALUE=urgent cargo run -p nx-cli -- run \
  examples/distributed_tags/target/wasm32-unknown-unknown/release/distributed_tags.wasm \
  --listen 127.0.0.1:9301 \
  --peer 127.0.0.1:9302 \
  --datastore-path examples/distributed_tags/data-a \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-orset tags:doc-1 \
  -v
```

Run Node B in list mode so it stays online long enough to receive the remove:

```bash
NX_TAG_ACTION=list cargo run -p nx-cli -- run \
  examples/distributed_tags/target/wasm32-unknown-unknown/release/distributed_tags.wasm \
  --listen 127.0.0.1:9302 \
  --peer 127.0.0.1:9301 \
  --datastore-path examples/distributed_tags/data-b \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-orset tags:doc-1 \
  -v
```

Expected final value on both nodes:

```text
tags:doc-1 = [review]
```

The remove only removes add-tags that Node A has observed. Concurrent add-tags
that were not observed by that remove would remain visible after convergence.

## Three Nodes

Start Node A and Node B as above, then add a third tag from Node C:

```bash
NX_TAG_ACTION=add NX_TAG_VALUE=backend cargo run -p nx-cli -- run \
  examples/distributed_tags/target/wasm32-unknown-unknown/release/distributed_tags.wasm \
  --listen 127.0.0.1:9303 \
  --peer 127.0.0.1:9301 \
  --peer 127.0.0.1:9302 \
  --datastore-path examples/distributed_tags/data-c \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-orset tags:doc-1 \
  -v
```

Expected final value after all three adds converge:

```text
tags:doc-1 = [backend, review, urgent]
```

## Reset

Remove the local example datastores before a clean run:

```bash
rm -rf examples/distributed_tags/data-a \
       examples/distributed_tags/data-b \
       examples/distributed_tags/data-c
```

## Notes

- ORSet means "observed-remove set": removes carry the add-tags observed by the
  remover.
- Concurrent adds survive removes that did not observe them.
- `--wait-before-run` gives peers time to connect before the module emits its
  operation.
- `--settle-for` keeps each runtime alive long enough for PushOps and
  anti-entropy recovery to apply remote operations before printing the final
  value.
- `--print-orset tags:doc-1` reads the host-side visible elements after settle,
  so the result does not depend on guest logs.
- Reusing datastores is intentional for restart/recovery checks. Use the reset
  command when you want a fresh scenario.
- Without `--settle-for`, a sync-enabled runtime stays alive until it receives
  a shutdown signal.
