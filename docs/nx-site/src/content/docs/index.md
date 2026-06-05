---
title: Numax
description: A portable runtime for local-first distributed apps. WebAssembly modules, local state, CRDT sync.
template: splash
hero:
  tagline: Run WebAssembly modules. Keep state local. Let CRDTs handle the rest.
  actions:
    - text: Quickstart in 5 minutes
      link: /getting-started/quickstart-5-min/
      icon: right-arrow
      variant: primary
    - text: Read the whitepaper
      link: /whitepaper/
      icon: open-book
      variant: secondary
    - text: GitHub
      link: https://github.com/GianIac/numax
      icon: external
      variant: minimal
---
## What is Numax

Numax is a small, portable runtime for distributed applications.
You write a WebAssembly module. Numax runs it in a sandbox, gives it a local
key/value store, and keeps that state in sync across nodes using CRDTs and
gossip.


## Three things, only three

1. **Sandboxed WebAssembly execution.**
   Your code runs isolated, on any host that speaks WASM. Rust today,
   more guest languages on the way through the Component Model.

2. **Local embedded datastore.**
   State lives next to the code in a durable key/value store using sled.

3. **CRDT sync over gossip.**
   Peers exchange operations and converge automatically.
   You don't write reconciliation code.

## What you can build today

The runtime ships with seven tiny examples, usually under 100 lines each.
They prove it works, give you something to copy, and make every replication
primitive concrete. Treat them as **starting points, not the ceiling**.

- **[`distributed_counter`](https://github.com/GianIac/numax/tree/main/examples/distributed_counter)** - replicated visit counter.
- **[`distributed_inventory`](https://github.com/GianIac/numax/tree/main/examples/distributed_inventory)** - restock / sale / return on a shared SKU.
- **[`distributed_status`](https://github.com/GianIac/numax/tree/main/examples/distributed_status)** - service status, last-writer-wins.
- **[`distributed_settings`](https://github.com/GianIac/numax/tree/main/examples/distributed_settings)** - replicated configuration map.
- **[`distributed_tags`](https://github.com/GianIac/numax/tree/main/examples/distributed_tags)** - collaborative tag set, observed-remove.
- **[`distributed_comments`](https://github.com/GianIac/numax/tree/main/examples/distributed_comments)** - ordered comments / collaborative text.
- **[`vote_tally_tls`](https://github.com/GianIac/numax/tree/main/examples/vote_tally_tls)** - three-node counter with mTLS.

### What you can really do with this

With the same building blocks people are already imagining:

- **Apps that follow you between devices** - notes, lists, trackers that work offline and just sync.
- **Things you make together in real time** - docs, whiteboards, small multiplayer games.
- **Software that keeps working when the network doesn't** - point-of-sale, edge inventory, festivals.
- **Sensors and devices that talk to each other** - homes, greenhouses, weather stations, fleets.
- **Configuration and feature flags that propagate themselves** - change once, every node picks it up.
- **…and a lot of things we honestly haven't thought of yet.**

That last one is the point. If your idea sounds like
*"many people or devices sharing something that always agrees with itself in the end"*,
Numax is probably one of the simplest tools you have for the job today.

### Built something? Show us.

Serious, silly, half-finished - doesn't matter.
If you wrote it, we'd love to see it on the [**Showcase**](/showcase/).

## Where we're going

The `0.1.x` line is a series of small, stable steps toward **`v0.2.0`** -
the version we want to recommend without footnotes, for any criticality.
The compass:

- **Dynamic peer discovery** - mDNS, DNS-SRV, file-watch, then SWIM membership and adaptive K-fanout gossip. No more `--peer 1.2.3.4` everywhere.
- **Reactive modules** - long-running event loops with `on_remote_op`, `on_tick`, `on_peer_connected`, `on_message`, and HTTP handlers. `run()` one-shot stays, but it stops being the only model.
- **Hot reload** - replace a module live without dropping peer connections or losing CRDT state.
- **Capability-based security** - per-module policies with deny-by-default capabilities and resource quotas (CPU, memory, ops/s, bytes/s). Multi-tenant safe.
- **Operability** - snapshot, restore, replay, diff, op-log compaction, deterministic mode for reproducible debugging.
- **Built-in dashboard and TUI** - six focused views for cluster, CRDTs, op flow, convergence health, throughput and modules, plus a `nx top` TUI for SSH-only setups.
- **Component Model + WIT** - a stable, multi-language ABI. Rust, Go (TinyGo), JavaScript and Python guests all converging on the same CRDT.

The full plan, version by version, lives in the
[**Roadmap**](/roadmap/). Roadmap PRs are as welcome as code PRs.

## Try it. Break it. Tell us.

Numax is in the moment where focused feedback shapes the project the most.

- **[Quickstart in 5 minutes](/getting-started/quickstart-5-min/)** - clone, build, run two converging nodes.
- **[Your first module](/getting-started/your-first-module/)** - the smallest interesting WASM module you can write.
- **[Open an issue](https://github.com/GianIac/numax/issues/new)** - bugs, design opinions, even small surprises.
- **[Star the repo](https://github.com/GianIac/numax)** - right now it's the simplest signal that this idea is worth pushing further.

Apache 2.0. One door, currently open.