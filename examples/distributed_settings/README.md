# Distributed Settings Example

LWW-Map-backed service settings replicated across Numax nodes.

This example models a distributed settings document. Each field is resolved
independently with last-writer-wins ordering, while removes are stored as
tombstones so old writes cannot resurrect deleted fields after reconnect.

## What It Demonstrates

- LWW-Map host API: `crdt_lww_map_set`, `crdt_lww_map_remove`,
  `crdt_lww_map_get`, `crdt_lww_map_contains`, `crdt_lww_map_entries`
- SDK wrapper: `nx_sdk::crdt::lww_map`
- host-side convergence output with `--print-lww-map`
- independent per-field convergence
- last writer wins on the same field
- remove tombstones for deleted fields
- durable local state and anti-entropy friendly op-log behavior

## Build

From this example directory:

```bash
cd examples/distributed_settings
cargo build --release --target wasm32-unknown-unknown
```

The module will be written to:

```text
target/wasm32-unknown-unknown/release/distributed_settings.wasm
```

## Scenario

All nodes operate on the same key by default:

```text
settings:service-a
```

Each run is configured through environment variables:

| Env var | Meaning | Default |
|---------|---------|---------|
| `NX_SETTING_KEY` | LWW-Map key | `settings:service-a` |
| `NX_SETTING_ACTION` | `set`, `remove`, or `list` | `set` |
| `NX_SETTING_FIELD` | field affected by `set` or `remove` | `theme` |
| `NX_SETTING_VALUE` | value used by `set` | `dark` |

Useful fields:

```text
theme
region
feature.checkout
feature.search
rollout.percent
```

## Two Nodes: Independent Fields

Run these from the repository root after building the WASM module. Start both
commands in separate terminals.

### TOML config alternative

Use one file per node for stable network/storage settings:

```bash
cat > examples/distributed_settings/node-a.toml <<'EOF'
[storage]
datastore_path = "examples/distributed_settings/data-a"

[network]
listen = "127.0.0.1:9401"
peers = ["127.0.0.1:9402"]
serialization_format = "bincode"

[discovery]
mode = "static"
EOF

cat > examples/distributed_settings/node-b.toml <<'EOF'
[storage]
datastore_path = "examples/distributed_settings/data-b"

[network]
listen = "127.0.0.1:9402"
peers = ["127.0.0.1:9401"]
serialization_format = "bincode"

[discovery]
mode = "static"
EOF

cargo run -p nx-cli -- config validate --config examples/distributed_settings/node-a.toml
```

Then run the independent-field scenario with shorter commands:

```bash
NX_SETTING_ACTION=set NX_SETTING_FIELD=theme NX_SETTING_VALUE=dark cargo run -p nx-cli -- run \
  examples/distributed_settings/target/wasm32-unknown-unknown/release/distributed_settings.wasm \
  --config examples/distributed_settings/node-a.toml \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-lww-map settings:service-a \
  -v
```

```bash
NX_SETTING_ACTION=set NX_SETTING_FIELD=region NX_SETTING_VALUE=eu cargo run -p nx-cli -- run \
  examples/distributed_settings/target/wasm32-unknown-unknown/release/distributed_settings.wasm \
  --config examples/distributed_settings/node-b.toml \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-lww-map settings:service-a \
  -v
```

CLI flags and `NX_*` variables override the file when both are present.

### Node A: set theme

```bash
NX_SETTING_ACTION=set NX_SETTING_FIELD=theme NX_SETTING_VALUE=dark cargo run -p nx-cli -- run \
  examples/distributed_settings/target/wasm32-unknown-unknown/release/distributed_settings.wasm \
  --listen 127.0.0.1:9401 \
  --peer 127.0.0.1:9402 \
  --datastore-path examples/distributed_settings/data-a \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-lww-map settings:service-a \
  -v
```

### Node B: set region

```bash
NX_SETTING_ACTION=set NX_SETTING_FIELD=region NX_SETTING_VALUE=eu cargo run -p nx-cli -- run \
  examples/distributed_settings/target/wasm32-unknown-unknown/release/distributed_settings.wasm \
  --listen 127.0.0.1:9402 \
  --peer 127.0.0.1:9401 \
  --datastore-path examples/distributed_settings/data-b \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-lww-map settings:service-a \
  -v
```

Expected final value on both nodes:

```text
settings:service-a = {region=eu, theme=dark}
```

## Same Field: Last Writer Wins

After the first scenario has converged, run two updates for the same field using
the same datastores:

```bash
NX_SETTING_ACTION=set NX_SETTING_FIELD=theme NX_SETTING_VALUE=light cargo run -p nx-cli -- run \
  examples/distributed_settings/target/wasm32-unknown-unknown/release/distributed_settings.wasm \
  --listen 127.0.0.1:9401 \
  --peer 127.0.0.1:9402 \
  --datastore-path examples/distributed_settings/data-a \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-lww-map settings:service-a \
  -v
```

Keep Node B online in list mode:

```bash
NX_SETTING_ACTION=list cargo run -p nx-cli -- run \
  examples/distributed_settings/target/wasm32-unknown-unknown/release/distributed_settings.wasm \
  --listen 127.0.0.1:9402 \
  --peer 127.0.0.1:9401 \
  --datastore-path examples/distributed_settings/data-b \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-lww-map settings:service-a \
  -v
```

Expected final value:

```text
settings:service-a = {region=eu, theme=light}
```

## Remove A Field

Remove `region` from Node B:

```bash
NX_SETTING_ACTION=remove NX_SETTING_FIELD=region cargo run -p nx-cli -- run \
  examples/distributed_settings/target/wasm32-unknown-unknown/release/distributed_settings.wasm \
  --listen 127.0.0.1:9402 \
  --peer 127.0.0.1:9401 \
  --datastore-path examples/distributed_settings/data-b \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-lww-map settings:service-a \
  -v
```

Run Node A in list mode while Node B removes the field:

```bash
NX_SETTING_ACTION=list cargo run -p nx-cli -- run \
  examples/distributed_settings/target/wasm32-unknown-unknown/release/distributed_settings.wasm \
  --listen 127.0.0.1:9401 \
  --peer 127.0.0.1:9402 \
  --datastore-path examples/distributed_settings/data-a \
  --wait-before-run 1500ms \
  --settle-for 4s \
  --print-lww-map settings:service-a \
  -v
```

Expected final value:

```text
settings:service-a = {theme=light}
```

The remove is a tombstone. An older write for `region` arriving later will not
make the field visible again.

## Reset

Remove the local example datastores before a clean run:

```bash
rm -rf examples/distributed_settings/data-a \
       examples/distributed_settings/data-b
```

## Notes

- LWW-Map resolves each field independently.
- The host assigns timestamps and uses the local NodeId as writer identity.
- If two writes have the same timestamp, writer NodeId is the deterministic
  tie-breaker.
- `--wait-before-run` gives peers time to connect before the module emits its
  operation.
- `--settle-for` keeps each runtime alive long enough for PushOps and
  anti-entropy recovery to apply remote operations before printing the final
  value.
- `--print-lww-map settings:service-a` reads the host-side visible entries
  after settle, so the result does not depend on guest logs.
- Reusing datastores is intentional for restart/recovery checks. Use the reset
  command when you want a fresh scenario.
