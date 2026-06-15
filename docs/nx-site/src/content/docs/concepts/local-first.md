---
title: Local-First
description: Why Numax keeps state next to the code.
---

Local-first is a design constraint that shapes everything: where state lives, how the network is treated, what happens when connectivity is lost, and which guarantees your application can provide without asking a remote server for permission.

This page explains what local-first means in Numax and why it matters.

---

## The default assumption most software makes

Most applications are built on a remote-first assumption. Authoritative state lives on a server. The client requests it, modifies it, and sends it back. The server decides what is true.

That works well when:

- the network is fast and reliable, or latency does not matter
- the server is always reachable
- you are happy to route every operation through centralized infrastructure :)

In practice, none of those conditions holds all the time. Networks fail. Servers have downtime. Latency matters for edge nodes, mobile devices, IoT sensors, and anything running far from a datacenter. Centralized infrastructure also has a cost: operational, financial, and architectural.

The remote-first model pushes complexity outward. Application logic stays simple only because hard problems like availability, consistency, and partition tolerance are delegated to infrastructure somewhere else. That delegation has a price.

---

## What local-first means

Local-first means the application works correctly with the data it has locally, without requiring a round trip to a remote system for every operation.

The canonical description from [Ink & Switch](https://www.inkandswitch.com/local-first/) is:

> In local-first software, the primary copy of your data lives on your local device.

For Numax, that means:

- the state store is embedded in the runtime process, not a remote service
- reads and writes go to sled, on disk, on the same machine as the code
- the network is used for synchronization, not for ordinary access
- a node that has never connected to a peer can still read and write local state
- replicated CRDT APIs are local-first once sync is enabled; sync is the replication layer, not the storage layer

A node does not merely degrade gracefully when it is offline. For its own local state, offline is not an error condition. It is the baseline.

---

## How Numax implements it

Every Numax node owns its data:

```
┌─────────────────────────────────────┐
│           Numax node                │
│                                     │
│  ┌──────────────┐  ┌─────────────┐  │
│  │ WASM module  │  │  nx-store   │  │
│  │  (compute)   │◄─┤  (sled)     │  │
│  └──────────────┘  └─────────────┘  │
│                                     │
│  ┌──────────────────────────────┐   │
│  │  nx-sync + nx-net (optional) │   │
│  │  CRDT sync with known peers  │   │
│  └──────────────────────────────┘   │
└─────────────────────────────────────┘
```

The module reads and writes through an embedded store. There is no remote connection to open, no query to send across the network, and no acknowledgement to wait for from another system before local state can move forward.

Sync is opt-in. A node started without `--listen` does not connect to peers. It simply runs. When sync is enabled, CRDTs handle convergence: local and remote state are merged using mathematical properties that guarantee eventual consistency without coordination.

The key point is: **synchronization is decoupled from access**. You do not need to be connected to read or write local state. You need to be connected to propagate changes to other nodes. Those are different concerns, and treating them as the same thing is a major source of fragility in remote-first systems.

---

## The tradeoffs

Local-first is not free. It moves complexity from infrastructure into the data model. Understanding the tradeoffs matters.

**What you gain:**

- **Availability.** The node works independently of the network. An edge gateway that loses its uplink for an hour can still accept writes and serve reads. When it reconnects, it syncs.
- **Latency.** Local reads and writes avoid network round trips. The state path stays close to the computation.
- **Resilience.** There is no central single point of failure. If one node stops, the others keep operating independently.
- **Deployment simplicity.** A Numax node is a binary, a config file, and a `.wasm` module. There is no remote database to provision, no connection string to manage, and no cloud dependency required for local operation.

**What you lose:**

- **Strong consistency.** You cannot have one globally agreed "current value" without coordination. CRDTs give eventual consistency: all nodes converge given enough time and connectivity, but different nodes may temporarily see different values.
- **Linearizability.** If you need "this value must be exactly X before I proceed, and no other node may change it while I decide", local-first cannot give you that without a coordination protocol above it.
- **Conflict-free mutations for every data shape.** CRDTs cover specific patterns: counters, registers, sets, maps, and sequences. If your data model needs patterns that do not map to those, local-first with CRDTs may not be the right fit.

---

## When local-first is the right choice

Local-first fits systems where:

- **writes happen independently on multiple nodes** and must converge later
- **offline operation is a real requirement**, not just a nice-to-have
- **latency matters**, because data is close to compute
- **the deployment target is heterogeneous**: servers, edge, IoT, mobile
- **the data model fits CRDTs**: counters, presence, tags, settings, ordered content

---

## When it is not the right choice

Local-first with CRDTs is not appropriate when:

- **strict operation ordering is required** and must be globally enforced
- **application logic must read a globally consistent value before writing**
- **data must become immediately visible to every node after a write** with no tolerance for propagation delay
- **the data model is fundamentally relational** with complex cross-entity constraints that the available CRDTs cannot express

These are not bugs. They are the honest boundary of the model. Knowing where it does not apply is as important as knowing where it does.

---

## Local-first and Numax philosophy

The local-first constraint is what makes Numax's three-component model coherent.

If state were remote, every read would require a network round trip. WASM sandbox isolation would mean much less if every operation depended on external I/O. The CRDT model would be a curiosity instead of a core part of the design.

Because state is local, the module runs close to its data. Because sync is separate from access, the node survives disconnections. Because CRDTs handle convergence, reconnection can be automatic and correct.

The result is a genuinely self-sufficient node. Not degraded, not cached, not a read replica. It is the primary node for its own data, always.

---

## Related

- [Runtime model](/numax/concepts/runtime-model/) - lifecycle and the three components
- [CRDT and state](/numax/concepts/crdt-and-state/) - how convergence works without coordination
- [Gossip protocol](/numax/concepts/gossip-protocol/) - how synchronization propagates between nodes
- [Ink & Switch: Local-first Software](https://www.inkandswitch.com/local-first/) - the original research that named this approach
