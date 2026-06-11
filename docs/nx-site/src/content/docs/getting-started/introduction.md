---
title: Introduction
description: What Numax is and when to use it.
---

## What Numax is

Numax is a runtime for distributed applications, written in Rust.

Three things, only three:

1. **Runs WebAssembly modules in a sandbox.**
   You write a module in any language that compiles to WASM. Numax loads it,
   links the host API, and calls `run()`. The module cannot touch anything outside
   what the host explicitly exposes.

2. **Keeps state local.**
   Every node has an embedded key/value datastore (sled) on disk.
   State lives next to the code, not on a remote server.
   The node stays useful even when the network is gone.

3. **Syncs state across nodes with CRDTs and gossip.**
   When nodes are connected, they exchange operations and converge automatically.
   You don't write reconciliation code. You don't resolve conflicts by hand.
   The data structures handle it.

That's the whole model. You write the logic. Numax handles the environment.

---

## Why it exists

Building distributed software today usually means:
containers, orchestrators, remote databases, a separate sync layer,
and three different toolchains depending on where the code runs.

Most of that weight exists to solve problems that come from the architecture itself,
not from the original problem you were trying to solve.

Numax tries a different path: keep the runtime small, keep the state local,
let CRDTs handle convergence. The hard parts of distributed systems don't disappear,
but you stop paying for the ones you didn't actually need.

---

## When to use it

Numax is a good fit when:

- you have **multiple nodes or devices that share state** and need to stay in sync
- you need the system to **keep working when the network is down or slow**
- you want to **ship logic as a WASM module** and let the runtime handle storage and sync
- you want something **self-contained** - no external database, no coordination service

Concrete examples: edge inventory systems, offline-capable apps, sensor networks,
collaborative tools, config propagation across nodes, small multiplayer state.

---

## When not to use it (for now)

- **Strong consistency or transactions across nodes** - CRDTs converge eventually, not immediately.
  This is on the roadmap.
- **Data models that don't fit CRDT semantics** - grow-only, last-writer-wins, set operations.
  More primitives are coming.
- **General-purpose database with rich queries** - not what Numax is.
- **Critical production workloads** - Numax is at `v0.1.x`, tested and usable, but still early.
  The remaining limits are documented in the [Roadmap](/roadmap/).

These are current limits, not permanent ones. If something is blocking you,
[open an issue](https://github.com/GianIac/numax/issues/new) - that's exactly how priorities get shaped.

---

## What's inside

| Piece | What it does |
|---|---|
| `nx` | The CLI - `nx run module.wasm`, `nx config validate`, `nx config show` |
| `nx-core` | The runtime engine - WASM loading, host API, datastore, sync |
| `nx-sdk` | The guest SDK - Rust library for writing WASM modules |
| `nx-site` | This documentation site |
| `examples/` | Seven working examples, each under ~100 lines |

WASM execution is powered by [Wasmtime](https://wasmtime.dev/).
The local datastore uses [sled](https://github.com/spacejam/sled).
Sync uses gossip with periodic anti-entropy for recovery.

---

## Current status

`v0.1.x` - first stable release line, intended for controlled and non-critical workloads.

It works. The examples run. The two nodes converge.
The remaining limits are documented in the [Roadmap](/roadmap/).

---

## Where to go next

- Never touched Numax before - [Quickstart: 5 Minutes](/getting-started/quickstart-5-min/)
- Want to write a module - [Your First Module](/getting-started/your-first-module/)
- Words like CRDT or gossip are new - [Foundations](/concepts/foundations/)
- Want to understand the full vision - [Whitepaper](/whitepaper/)
- Want to see where the project is going - [Roadmap](/roadmap/)