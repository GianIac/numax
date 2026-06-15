---
title: Gossip Protocol
description: How Numax moves operations between peers.
---

This page explains what gossip means in Numax, what the current sync layer already does, and what will arrive in the peer-discovery releases.

The short version: **today Numax uses configured peers, direct broadcasts and periodic anti-entropy.** Future releases will turn that into dynamic peer discovery with SWIM-style membership and K-fanout gossip.

---

## What gossip is

A gossip protocol is a way to spread information through a distributed system without requiring one central coordinator.

Instead of sending every update through a leader, each node talks to some peers. Those peers talk to other peers. Over time, the information spreads through the cluster.

For Numax, the information being spread is mostly CRDT operations:

```rust
pub struct Op {
    pub id:     OpId,
    pub origin: NodeId,
    pub kind:   OpKind,
}
```

Each operation has a globally unique `OpId`, the node that produced it, and the CRDT change itself. Peers use `OpId` to deduplicate messages they have already seen.

---

## What exists today

The current implementation is intentionally simple and deterministic.

Numax does not yet have dynamic peer discovery. A node knows the peers configured at startup or added explicitly through the runtime API. When an operation is produced locally, the sync manager queues it and sends it to the currently connected peers.

```
local CRDT host call
       |
       v
op queued in SyncManager
       |
       v
broadcast loop batches ops
       |
       v
PushOps sent to connected peers
       |
       v
peer applies unseen ops and persists state
```

This is not SWIM and it is not K-fanout yet. It is a full broadcast to connected peers, bounded by `max_peers`, with batching and backpressure through the operation queue.

---

## Wire messages

Peer communication is handled by `nx-net`. The current wire protocol defines these message kinds:

| Message | Purpose |
|---|---|
| `Hello` | Start the handshake, declare node id, protocol version and supported serialization formats. |
| `HelloAck` | Accept the handshake and choose the serialization format. |
| `PushOps` | Send one or more CRDT operations to a peer. |
| `PushOpsAck` | Acknowledgement message for received operations. The current sync path does not rely on it as a causal frontier. |
| `PullSince` | Ask a peer for retained operations. Today this is usually sent with `None`. |
| `Ping` / `Pong` | Keepalive message types. A received `Ping` is answered with `Pong`. |

The protocol version is currently `2`. Peers negotiate either `Bincode` or `Json`, with `Bincode` as the production default and `Json` available for debug-style interoperability.

---

## Handshake and identity

When a node connects to a peer, the first exchange is:

```
client -> server: Hello(node_id, version, supported_formats, preferred_format)
server -> client: HelloAck(node_id, version, selected_format)
```

After that, both sides know the peer `NodeId` and the selected serialization format.

If TLS is enabled, the claimed `NodeId` is checked against the peer certificate. This prevents a node from claiming an identity that does not match its certificate. Optional allowlists can further restrict which peer ids are accepted.

---

## Broadcast path

Local CRDT writes are applied locally first. Then the corresponding operation is queued for network propagation.

The broadcast loop drains that queue, groups operations into batches, records them in the seen-op and op-log metadata, and sends a `PushOps` message through `nx-net`.

The important boundaries are:

| Boundary | Current default |
|---|---|
| Maximum connected peers | `nx_net::DEFAULT_MAX_PEERS` |
| Queued local ops | `10,000` |
| Retained op-log entries | `10,000` |
| Retained seen `OpId`s | `100,000` |
| Socket timeout | `nx_net::DEFAULT_SOCKET_TIMEOUT` |

If a peer is disconnected, it does not receive the immediate push. That is why anti-entropy exists.

---

## Anti-entropy

Anti-entropy is the repair loop.

Every `anti_entropy_interval` seconds, a node asks each connected configured peer for retained operations using `PullSince`.

Today the request is conservative: it asks for the bounded op-log rather than relying on a single "last seen op id" as a causal frontier. That matters because one newer operation does not prove that every older operation arrived.

The receiving side deduplicates by `OpId`, applies only unseen operations, and persists the resulting CRDT state.

```
node A missed op-7 during a temporary disconnect
       |
       v
node A reconnects
       |
       v
anti-entropy sends PullSince(None)
       |
       v
node B returns retained ops
       |
       v
node A applies only unseen OpIds
```

The op-log is bounded, so anti-entropy is a practical catch-up mechanism, not an infinite historical archive.

---

## Peer health and reconnect

Configured peers have a small health state:

| State | Meaning |
|---|---|
| `Healthy` | The configured peer is connected or recently connected successfully. |
| `Suspect` | A connection attempt failed, but the peer has not crossed the failure threshold. |
| `Dead` | Consecutive failures reached `peer_dead_after_failures`. |

Reconnect uses exponential backoff:

| Setting | Current default |
|---|---|
| First reconnect delay | `500ms` |
| Maximum reconnect delay | `30s` |
| Dead after failures | `3` |
| Anti-entropy interval | `30s` |

This is simple failure tracking for configured peers. It is not a full membership protocol yet.

---

## What is not implemented yet

The current release line does **not** yet provide:

- automatic peer discovery,
- SWIM membership,
- Lifeguard-style failure detection,
- phi-accrual failure detection,
- K-fanout dissemination,
- adaptive gossip rate,
- NAT traversal,
- causal frontier metadata for precise incremental pulls.

If you see "gossip" in the current docs, read it as the sync layer that propagates and repairs CRDT operations between known peers. The more formal gossip protocol is planned in the peer-discovery work.

---

## What comes next

Peer discovery is planned in two steps.

### v0.1.5 - Peer Discovery: Foundations

This release introduces the discovery abstraction and the first discovery backends.

Planned work:

- `PeerDiscovery` trait with `discover()`, `announce()` and `watch()`.
- `StaticDiscovery`, preserving the current configured-peer behavior.
- Bootstrap discovery: join through one known address and learn other peers.
- mDNS discovery for LAN/dev setups.
- DNS-SRV discovery for environments that already publish service records.
- File-watch discovery for orchestrators and Kubernetes-style setups.

The goal is to stop making every node list every other node manually.

### v0.1.6 - Peer Discovery: SWIM & Gossip K-fanout

This is where the protocol becomes a real dynamic cluster protocol.

Planned split:

| Channel | Responsibility |
|---|---|
| Membership | SWIM / Lifeguard-style view of who is in the cluster. |
| Failure detection | Suspicion and dead-peer detection without relying only on configured addresses. |
| Data dissemination | K-fanout gossip for CRDT operations. |

K-fanout means a node does not send every update to every peer. Instead, it sends each update to `K` selected peers. Those peers forward it further. With a good value of `K`, the cluster gets fast propagation without every operation becoming a full-cluster broadcast.

The planned default is based on cluster size:

```text
K = ceil(log2(N) + c)
```

where `N` is the known cluster size and `c` is a small safety constant.

The roadmap also includes adaptive fanout based on load and RTT, controlled backpressure, periodic anti-entropy as a repair path, and seedable randomness so tests can reproduce gossip behavior.

---

## Why both gossip and anti-entropy

Gossip is the fast path. It spreads new operations quickly.

Anti-entropy is the repair path. It catches up nodes that were offline, partitioned, slow, or unlucky.

Numax needs both because local-first systems must tolerate temporary disconnection. CRDTs make the merge safe. Gossip moves operations quickly. Anti-entropy makes missed operations recoverable.

---

## Related

- [CRDT and state](/numax/concepts/crdt-and-state/) - the data model that makes convergence safe
- [Runtime model](/numax/concepts/runtime-model/) - how modules, host APIs and sync interact
- [nx-net crate](/numax/reference/crates/nx-net/) - peer transport and wire messages
- [nx-sync crate](/numax/reference/crates/nx-sync/) - operations, CRDTs and deduplication
- [Roadmap](/numax/roadmap/) - planned peer discovery releases
