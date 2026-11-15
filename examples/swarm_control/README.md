# Swarm Control

Shared tooling for the [`distributed_ants`](../distributed_ants/README.md) and
[`distributed_magnets`](../distributed_magnets/README.md) swarm demos. It is
not a standalone example.

## Contents

- `src/main.rs` — small Rust CLI. It reads a JSON object of `key -> value`
  from stdin and writes a WASM "control guest" to stdout. When run on a node,
  the guest writes those entries to the node's local DB via `nx.db_set`. The
  demo guests read them as configuration (`demo:env/NUMAX_*`), so the runner
  can change a running node's settings, clear `demo:completed`, or invalidate
  `demo:snapshot`.
  Only those three key families are accepted, with limits of 32 entries,
  128-byte keys, 4096-byte values and 64 KiB of input.
- `swarm_demo.py` — runner that starts a fleet of local `nx` nodes, uploads the
  demo guest, drives the ticks, takes snapshots and shuts everything down. It
  is launched by each demo's `demo.sh`.
- `test_swarm_demo.py` — unit tests for the runner helpers.

## Usage

Build the generator and `nx` first (from the repository root):

```sh
cargo build --manifest-path examples/swarm_control/Cargo.toml
cargo build --release -p nx-cli   # or point --nx / NX_BIN at an existing binary
```

Then run a demo through its script, for example:

```sh
examples/distributed_ants/demo.sh
examples/distributed_magnets/demo.sh
```

`swarm_demo.py` can also be called directly:

```sh
python3 examples/swarm_control/swarm_demo.py {ants|magnets} <config.toml> [--nx PATH] [--state DIR] [--timeout SECONDS]
```

## Tests

```sh
cargo test --manifest-path examples/swarm_control/Cargo.toml
python3 -m unittest discover -s examples/swarm_control -p test_swarm_demo.py
```
