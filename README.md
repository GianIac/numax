<p align="center">
  <img width="800" alt="NUMAX" src="https://github.com/user-attachments/assets/c628c233-8523-4abd-a57c-a15e32c6947a" />
</p>

# numax

[![Whitepaper](https://img.shields.io/badge/docs-whitepaper-blue)](./WHITEPAPER.md)
[![Roadmap](https://img.shields.io/badge/project-roadmap-orange)](./ROADMAP.md)

A portable runtime for distributed apps. Written in Rust.

Three things, and only three:

1. Runs WebAssembly modules in an isolated sandbox.
2. Has a local embedded key/value datastore, state lives next to the code.
3. Syncs state across nodes with CRDTs and gossip.

You write a WASM module. numax runs it. The state is there. Sync just happens.

> **Status:** `v0.1.0-alpha.2` - public technical preview.
> It works, it's tested, it's honest about what's missing. See [`ROADMAP.md`](./ROADMAP.md).

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

- [`WHITEPAPER.md`](./WHITEPAPER.md) - the vision, the architecture, the principles.
- [`ROADMAP_v0.1.0.md`](./ROADMAP.md) - where we are, where we're going, what's still missing.
- [`HOST_API.md`](./HOST_API.md) - the host API available to WASM modules.

---

## A small ask

If numax interests you - if you think the idea is worth something - drop a star !

Right now it's pretty much the only signal I have to understand whether this is worth pushing further.

---

## Try it. Break it. Tell me.

numax is in alpha. It's the moment where feedback matters most.

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
