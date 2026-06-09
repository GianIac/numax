<p align="center">
  <img width="800" alt="NUMAX" src="https://github.com/user-attachments/assets/c628c233-8523-4abd-a57c-a15e32c6947a" />
</p>

# numax

[![Whitepaper](https://img.shields.io/badge/docs-whitepaper-blue)](./docs/nx-site/src/content/docs/whitepaper/)
[![Roadmap](https://img.shields.io/badge/project-roadmap-orange)](./docs/nx-site/src/content/docs/roadmap/)

A portable runtime for distributed apps. Written in Rust.

Three things, and only three:

1. Runs WebAssembly modules in an isolated sandbox.
2. Has a local embedded key/value datastore, state lives next to the code.
3. Syncs state across nodes with CRDTs and gossip.

You write a WASM module. numax runs it. The state is there. Sync just happens.

> **Status:** `v0.1.0` - first stable Numax release line for controlled, non-critical workloads.
> It works, it's tested, and the remaining limits are documented. See the [`Roadmap`](./docs/nx-site/src/content/docs/roadmap/).

---

## Why

Building distributed software today is heavier than the problem it's trying to solve.
Containers, orchestrators, remote databases, ad-hoc sync layers, three different toolchains depending on where the code runs.

numax tries a different path: keep the runtime tiny, keep the state local, let CRDTs handle convergence.
The hard parts of distributed systems don't disappear, but you stop paying for the ones you didn't actually need.

---

## Quickstart

For now, build from source:

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo build --release
```

Run the `distributed_counter` example on two nodes:

```bash
# Node A
nx run distributed_counter.wasm \
    --listen 0.0.0.0:9000 \
    --datastore-path ./data-a -v

# Node B
nx run distributed_counter.wasm \
    --listen 0.0.0.0:9001 \
    --peer 127.0.0.1:9000 \
    --datastore-path ./data-b -v
```

The two nodes find each other, sync, and converge. You don't have to do anything else.

You can also move the node settings into TOML files and keep the command line
focused on run-specific options:

```toml
# node-a.toml
[storage]
datastore_path = "./data-a"

[network]
listen = "0.0.0.0:9000"
peers = ["127.0.0.1:9001"]
serialization_format = "bincode"

[discovery]
mode = "static"
```

```bash
nx config validate --config node-a.toml
nx config show --config node-a.toml --effective

nx run distributed_counter.wasm \
    --config node-a.toml \
    --settle-for 5s \
    --print-gcounter counter:visits
```

Runtime precedence is explicit: CLI flags override `NX_*` environment
variables, environment variables override the TOML file, and the file overrides
runtime defaults. The distributed examples below include full two-node TOML
setups.

---

## Writing a guest module

A minimal module using the local datastore:

```rust
use nx_sdk::{db, log};

#[no_mangle]
pub extern "C" fn run() {
    db::set("hello", b"numax").unwrap();
    log("done.");
}
```

Or with a replicated CRDT counter:

```rust
use nx_sdk::{log, crdt::gcounter};

#[no_mangle]
pub extern "C" fn run() {
    gcounter::inc("visits", 1).unwrap();
    let v = gcounter::value("visits").unwrap();
    log(&format!("visits: {}", v));
}
```

Same module, any node. State stays local. Sync happens through the runtime.

---

## Learn more

- [`Whitepaper`](./docs/nx-site/src/content/docs/whitepaper/) - the vision, the architecture, the principles.
- [`Roadmap`](./docs/nx-site/src/content/docs/roadmap/) - where we are, where we're going, what's still missing.
- [`Host API`](./docs/nx-site/src/content/docs/reference/host-api.md) - the host API available to WASM modules.
- [`examples/distributed_inventory`](./examples/distributed_inventory) - replicated PNCounter inventory.
- [`examples/distributed_status`](./examples/distributed_status) - replicated LWW-Register status.
- [`examples/distributed_tags`](./examples/distributed_tags) - replicated ORSet tags.
- [`examples/distributed_settings`](./examples/distributed_settings) - replicated LWW-Map settings.
- [`examples/distributed_comments`](./examples/distributed_comments) - replicated RGA comments.

---

## A small ask

If numax interests you - if you think the idea is worth something - drop a star !

Right now it's pretty much the only signal I have to understand whether this is worth pushing further.

---

## Try it. Break it. Tell me.

numax is in its first stable release line. It is usable, but still early: focused feedback matters a lot.

- Clone it, run the examples, see if the two nodes really converge on your machine.
- Write a tiny module of your own and try to break the sandbox or the sync.
- If something behaves in a way you didn't expect - open an issue. Even a small one. Especially a small one.
- If you have an opinion on the design, the host API, the CRDT model - open an issue for that too.

There's no community to pretend already exists. There's a project, an idea, and a door that's open.
If you walk through it now, you're early. That's the best moment to leave a mark.

ps: If you'd like to help, take a look at [CONTRIBUTING.md](./CONTRIBUTING.md).

- GianIac

---

**License:** Apache 2.0
