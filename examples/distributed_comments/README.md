# Distributed Comments Example

RGA-backed ordered comments replicated across Numax nodes.

This example models a collaborative comment stream. Inserts create stable
element ids, deletes tombstone ids, and children inserted after a deleted parent
remain visible.

## What It Demonstrates

- RGA host API: `crdt_rga_insert`, `crdt_rga_delete`, `crdt_rga_values`
- SDK wrapper: `nx_sdk::crdt::rga`
- stable element ids returned by insert
- ordered convergence across nodes
- tombstone deletes without removing child elements
- host-side convergence output with `--print-rga`

## Build

From this example directory:

```bash
cd examples/distributed_comments
cargo build --release --target wasm32-unknown-unknown
```

The module will be written to:

```text
target/wasm32-unknown-unknown/release/distributed_comments.wasm
```

## Scenario

All nodes operate on the same key by default:

```text
comments:doc-1
```

Each run is configured through environment variables:

| Env var | Meaning | Default |
|---------|---------|---------|
| `NX_COMMENT_KEY` | RGA key | `comments:doc-1` |
| `NX_COMMENT_ACTION` | `append`, `insert`, `delete`, or `list` | `append` |
| `NX_COMMENT_TEXT` | value used by `append` or `insert` | `hello from numax` |
| `NX_COMMENT_PARENT` | element id to insert after | unset, inserts at head |
| `NX_COMMENT_ID` | element id to delete | unset |

## Two Nodes: Append And Converge

Run these from the repository root after building the WASM module. Start both
commands in separate terminals.

### TOML config alternative

Use one file per node for stable network/storage settings:

```bash
cat > examples/distributed_comments/node-a.toml <<'EOF'
[storage]
datastore_path = "examples/distributed_comments/data-a"

[network]
listen = "127.0.0.1:9701"
peers = ["127.0.0.1:9702"]
serialization_format = "bincode"

[discovery]
mode = "static"
EOF

cat > examples/distributed_comments/node-b.toml <<'EOF'
[storage]
datastore_path = "examples/distributed_comments/data-b"

[network]
listen = "127.0.0.1:9702"
peers = ["127.0.0.1:9701"]
serialization_format = "bincode"

[discovery]
mode = "static"
EOF

cargo run -p nx-cli -- config validate --config examples/distributed_comments/node-a.toml
```

Then run the append scenario with shorter commands:

```bash
NX_COMMENT_ACTION=append NX_COMMENT_TEXT="first comment" cargo run -p nx-cli -- run \
  examples/distributed_comments/target/wasm32-unknown-unknown/release/distributed_comments.wasm \
  --config examples/distributed_comments/node-a.toml \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-rga comments:doc-1 \
  -v
```

```bash
NX_COMMENT_ACTION=append NX_COMMENT_TEXT="second comment" cargo run -p nx-cli -- run \
  examples/distributed_comments/target/wasm32-unknown-unknown/release/distributed_comments.wasm \
  --config examples/distributed_comments/node-b.toml \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-rga comments:doc-1 \
  -v
```

CLI flags and `NX_*` variables override the file when both are present.

### Node A: append first comment

```bash
NX_COMMENT_ACTION=append NX_COMMENT_TEXT="first comment" cargo run -p nx-cli -- run \
  examples/distributed_comments/target/wasm32-unknown-unknown/release/distributed_comments.wasm \
  --listen 127.0.0.1:9701 \
  --peer 127.0.0.1:9702 \
  --datastore-path examples/distributed_comments/data-a \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-rga comments:doc-1 \
  -v
```

Copy the logged `inserted id=...` value from Node A when you want to insert
after that comment in the next scenario.

### Node B: append second comment

```bash
NX_COMMENT_ACTION=append NX_COMMENT_TEXT="second comment" cargo run -p nx-cli -- run \
  examples/distributed_comments/target/wasm32-unknown-unknown/release/distributed_comments.wasm \
  --listen 127.0.0.1:9702 \
  --peer 127.0.0.1:9701 \
  --datastore-path examples/distributed_comments/data-b \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-rga comments:doc-1 \
  -v
```

Expected final value on both nodes contains both comments in deterministic RGA
order:

```text
comments:doc-1 = [first comment, second comment]
```

If both nodes append at the head concurrently, the order is deterministic by
element id. That is expected and convergence-safe.

## Insert After A Known Comment

Use the id logged by Node A as `NX_COMMENT_PARENT`:

```bash
NX_COMMENT_ACTION=insert NX_COMMENT_PARENT=<node-a-insert-id> NX_COMMENT_TEXT="reply to first" cargo run -p nx-cli -- run \
  examples/distributed_comments/target/wasm32-unknown-unknown/release/distributed_comments.wasm \
  --listen 127.0.0.1:9702 \
  --peer 127.0.0.1:9701 \
  --datastore-path examples/distributed_comments/data-b \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-rga comments:doc-1 \
  -v
```

The reply is ordered after the parent on every node once sync settles.

## Delete A Comment

Delete the original first comment by id:

```bash
NX_COMMENT_ACTION=delete NX_COMMENT_ID=<node-a-insert-id> cargo run -p nx-cli -- run \
  examples/distributed_comments/target/wasm32-unknown-unknown/release/distributed_comments.wasm \
  --listen 127.0.0.1:9701 \
  --peer 127.0.0.1:9702 \
  --datastore-path examples/distributed_comments/data-a \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-rga comments:doc-1 \
  -v
```

Any reply inserted after the deleted parent remains visible. The delete only
tombstones the targeted element id.

## Reset

Remove the local example datastores before a clean run:

```bash
rm -rf examples/distributed_comments/data-a \
       examples/distributed_comments/data-b
```

## Notes

- RGA is built for ordered sequences, not key/value replacement.
- Insert returns the element id because later insert-after and delete operations
  need a stable CRDT identity.
- `--wait-before-run` gives peers time to connect before the module emits its
  operation.
- `--settle-for` keeps each runtime alive long enough for PushOps and
  anti-entropy recovery to apply remote operations before printing the final
  value.
- `--print-rga comments:doc-1` reads host-side visible values after settle, so
  the result does not depend on guest logs.
