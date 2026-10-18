---
title: Gossip Protocol
description: How Numax moves operations between peers.
---

Numax discovers connection candidates, admits peers through the wire handshake,
and broadcasts accepted CRDT operations to active connections. Periodic
anti-entropy repairs missed operations through bounded replay or a verified
CRDT baseline. SWIM/Lifeguard membership and K-fanout are proposed extensions
for v0.1.6; the implemented transport still uses direct broadcast.

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

Each operation has a globally unique `OpId`, its origin and the CRDT change.
The v0.1.6 development replication envelope also carries a durable generation
and per-origin sequence. Receipt tracking suppresses delayed duplicates after
the bounded UUID cache expires; the largest UUID is not a completeness frontier.

---

## Replication architecture

A node obtains endpoint candidates from static, bootstrap, mDNS,
DNS-SRV or file providers. The same bounded, updateable candidate snapshot feeds
initial dialing and reconnect. Anti-entropy uses all active connections,
including inbound peers and connections whose candidates have been removed.
Starting with no candidates is valid; later provider updates wake the
connection machinery. Local readiness does not imply peer convergence.

Candidates are not members or peers yet. A candidate becomes an active peer
only after connection admission, the normal wire handshake, TLS identity
binding when configured, and allowlist authorization. When an operation is
produced locally, the sync manager queues it and sends it to the currently
connected peers.

![Current direct broadcast to connected peers](/numax/diagrams/concepts/current-broadcast.svg)

[Mermaid source for this diagram](/numax/diagrams/concepts/current-broadcast.mmd)

The diagram uses the legacy `PushOps` label. Protocol 8 carries sequenced
operations in the `History::Push` envelope with the same direct-broadcast topology.

Broadcast is bounded by `max_peers`, with batching and backpressure through
the operation queue. It does not maintain a SWIM membership view or select
K-fanout neighbors.

---

## Wire messages

Peer communication is handled by `nx-net`. The current wire protocol defines these message kinds:

| Message | Purpose |
|---|---|
| `Hello` | Start the handshake, declare node id, protocol version and supported serialization formats. |
| `HelloAck` | Accept the handshake and choose the serialization format. |
| `PushOps` | Legacy public codec; unsequenced replay is rejected by the development sync path. |
| `PushOpsAck` | Acknowledgement message for received operations. The current sync path does not rely on it as a causal frontier. |
| `PullSince` | Legacy public codec; development replication uses sequenced history requests instead. |
| `Ping` / `Pong` | Keepalive message types. A received `Ping` is answered with `Pong`. |
| `Error` | Structured wire error sent before rejecting a request or closing a connection. |
| `BootstrapHello` | Start a one-shot authenticated bootstrap request with cluster, advertisement and result limit. |
| `BootstrapAck` | Return the seed identity, matching cluster, negotiated format, bounded endpoint suggestions and their lease. |
| `History` | Private protocol-8 envelope for sequenced pushes, paginated summaries, correlated range requests/replay, unavailable-history responses and bounded baseline recovery. |

The v0.1.6 development protocol is `8`; the released v0.1.5 protocol is `5`.
Peers require equal versions before negotiating `Bincode` (implemented with
`wincode`) or `Json`. Preserving legacy codec bytes does not make protocol-5
peers compatible with protocol 8. See [Wire Versioning](/numax/design/wire-versioning/)
for rejection tests and deployment implications.

---

## Handshake and identity

When a node connects to a peer, the first exchange is:

![Normal peer handshake from transport connection to peer admission](/numax/diagrams/concepts/peer-handshake.svg)

[Mermaid source for this diagram](/numax/diagrams/concepts/peer-handshake.mmd)

The messages carry these fields:

```
client -> server: Hello(node_id, protocol_version, supported_formats, preferred_format)
server -> client: HelloAck(node_id, protocol_version, selected_format)
```

After that, both sides know the peer `NodeId` and the selected serialization format.

If TLS is enabled, the claimed `NodeId` is checked against the peer certificate. This prevents a node from claiming an identity that does not match its certificate. Optional allowlists can further restrict which peer ids are accepted.

### Protocol compatibility

Numax currently requires an exact protocol-version match, there is no implicit
forward or backward compatibility:

| Local node | Peer node | Result |
|---|---|---|
| `N` | `N` | Compatible; continue the handshake |
| `N` | `N - 1` | Incompatible; reject the handshake |
| `N` | `N + 1` | Incompatible; reject the handshake |

Both `Hello` and `HelloAck` carry an explicit `protocol_version`, a mismatch is
rejected before the peer is registered or CRDT operations are exchanged.
When possible, the rejecting peer sends `Error(ProtocolMismatch)` before closing.
Serialization-format negotiation does not override protocol compatibility.
The rules for evolving this contract are defined in
[Wire Versioning](/numax/design/wire-versioning/).

### Bootstrap handshake

Bootstrap uses the same listener but a separate, one-shot first message:

![One-shot bootstrap handshake with seed validation and endpoint suggestions](/numax/diagrams/concepts/bootstrap-handshake.svg)

[Mermaid source for this diagram](/numax/diagrams/concepts/bootstrap-handshake.mmd)

The one-shot messages carry these fields:

```text
client -> seed: BootstrapHello(
  node_id, protocol_version, supported_formats, preferred_format,
  cluster_id, advertised_endpoint?, max_results
)
seed -> client: BootstrapAck(
  node_id, protocol_version, selected_format,
  cluster_id, candidates, candidate_ttl_ms
)
connection closes
```

The seed authenticates the requesting node using the same TLS certificate
binding and allowlist checks as a normal peer handshake, validates cluster and
advertised endpoint, then records that endpoint under a bounded lease. The
client likewise authenticates the seed and validates the complete response.
Cluster mismatch or an invalid request yields `BootstrapRejected`.

The one-shot exchange never enters the active peer map and emits no
`PeerConnected` event. Authentication proves only who answered and who made the
request; it does not vouch for any endpoint in `candidates`. Each suggestion is
fed into normal reconnection and must authenticate independently before CRDT
traffic can flow. Response count, cache size, candidate TTL, message size,
socket time and concurrent client queries are all bounded.

---

## Broadcast path

Local CRDT success follows an atomic batch of
CRDT state, materialized value, UUID, seen/replay metadata and sequence position,
then a covering flush. Only then does the operation become eligible for network
propagation. The broadcast loop drains the bounded queue and sends sequenced
`History::Push` batches through `nx-net`; local success is not a remote ACK.
See the [acceptance contract](/numax/design/peer-discovery/#d1--local-acceptance-and-durability).

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

The development protocol periodically exchanges bounded, paginated per-origin
summaries with active connections. It compares known maxima against durable
contiguous receipt prefixes and sparse positions, then requests missing ranges.
Each request has an attempt ID and deadline; replies must match the source,
origin and requested interval. A summary advertises known existence, not durable
receipt or global convergence. Candidate removal stops future reconnects, not
repair over an already admitted connection.

Replay is paginated and bounded in bytes. Applying an operation persists its
state and receipt tracking together before publication. If a source lacks the
requested history it reports that interval unavailable; this does not prove
that every other source also lacks it.

The following diagram shows the older v0.1.5 bounded-log pull, not a trace of
protocol-8 messages:

![Historical v0.1.5 anti-entropy after a disconnect](/numax/diagrams/concepts/current-anti-entropy.svg)

[Mermaid source for this diagram](/numax/diagrams/concepts/current-anti-entropy.mmd)

Migrated UUID-only history is preserved locally and excluded from automatic
replay. Its `baseline_required` condition survives new sequenced writes and
restart; a fresh peer cannot infer historical convergence from new frontiers.
The [schema migration contract](/numax/design/schema-versioning/#upgrade-historical-state-and-downgrade-contract)
specifies this boundary. The log retains a configured count of operations
(10,000 by default), rather than a guaranteed duration. After expiry, protocol 8 can transfer a bounded, versioned
full CRDT baseline when the source cut covers the receiver's applied history and
state. The payload budget is 1 MiB including encoding overhead, so even a single
otherwise legal large guest value can exceed it. Incomplete sources, oversized
state and incomparable cuts fail explicitly; another source may still recover
the gap. See the [D3 contract](/numax/design/peer-discovery/#selected-bounded-recovery-contract).
The bounded log and baseline subset are not an unlimited recovery archive.

---

## Peer health and reconnect

Current candidates have a small health state:

| State | Meaning |
|---|---|
| `Healthy` | The candidate is connected or recently connected successfully. |
| `Suspect` | A connection attempt failed, but the peer has not crossed the failure threshold. |
| `Dead` | Consecutive failures reached `peer_dead_after_failures`. |

Reconnect uses exponential backoff:

| Setting | Current default |
|---|---|
| First reconnect delay | `500ms` |
| Maximum reconnect delay | `30s` |
| Dead after failures | `3` |
| Anti-entropy interval | `30s` |

This is simple failure tracking for discovery candidates, including configured
peers. It is not a full membership protocol yet.

---

## Proposed extensions

The following capabilities are outside the implemented discovery and replication path:

- SWIM membership,
- Lifeguard-style failure detection,
- phi-accrual failure detection,
- K-fanout dissemination,
- adaptive gossip rate,
- NAT traversal.

Versioned baseline recovery for legacy state and expired history is implemented
within the [D3 limits](/numax/design/peer-discovery/#selected-bounded-recovery-contract).
Larger snapshots and arbitrary incomparable-cut reconciliation remain outside
that contract.

Bootstrap gossip exchanges bounded endpoint suggestions. Data dissemination
uses admitted connections and direct broadcast, with anti-entropy for repair.
Bootstrap suggestions do not establish membership or data readiness.

---

## Discovery integration and proposed dissemination

### Discovery providers

The `PeerDiscovery` contract supports five providers: static configuration, authenticated bootstrap, LAN mDNS,
DNS-SRV and an externally updated peer file. Snapshot/watch handoff is atomic,
delivery is bounded with explicit overflow, and provider tasks are owned and
stopped by runtime shutdown.

All five modes (`static`, `bootstrap`, `mdns`, `dns-srv`, `file`) are selectable
through `--discovery-mode`, `NX_DISCOVERY_MODE` and the `[discovery]` TOML section,
with precedence CLI > environment > TOML > defaults. Explicit `--peer` entries
continue to contribute a static source alongside the selected dynamic provider;
they are not reinterpreted as bootstrap seeds. Embedders can also compose the
public providers through the `nx-core` Rust API. The detailed semantics are in the
[Peer Discovery Contract](/numax/design/discovery-contract/).

### Proposed membership and K-fanout

The proposed architecture separates three responsibilities:

| Channel | Responsibility |
|---|---|
| Membership | SWIM / Lifeguard-style view of who is in the cluster. |
| Failure detection | Suspicion and dead-peer detection without relying only on configured addresses. |
| Data dissemination | K-fanout gossip for CRDT operations. |

K-fanout selects `K` eligible peers for each update; those peers forward the
original operation after persistence. Duplicate suppression prevents repeated
effects and forwarding cycles. A bounded, rotating neighbor set and periodic
anti-entropy remain necessary for sparse topologies and healed partitions.

The planned default is based on cluster size:

```text
K = ceil(log2(N) + c)
```

where `N` is the local estimate of eligible membership and `c` is a calibrated
constant. The implementation must clamp `K` to available peers and configured
budgets, including empty and small clusters. The formula alone does not
establish connectivity or convergence.

The proposed scheduler includes load/RTT adaptation, bounded backpressure and
seeded randomness for deterministic simulation of the production state machine.
These parameters remain subject to the
[peer-discovery design](/numax/design/peer-discovery/).

---

## Why both gossip and anti-entropy

Gossip is the fast path. It spreads new operations quickly.

Anti-entropy is the repair path. It catches up nodes that were offline, partitioned, slow, or unlucky.

Numax needs both because local-first systems must tolerate temporary
disconnection. CRDTs define convergence semantics; dissemination moves
operations between peers. Anti-entropy repairs retained history by replay and
can establish a baseline after expiry within the declared state-transfer limits.

---

## Related

- [CRDT and state](/numax/concepts/crdt-and-state/) - the data model that makes convergence safe
- [Runtime model](/numax/concepts/runtime-model/) - how modules, host APIs and sync interact
- [nx-net crate](/numax/reference/crates/nx-net/) - peer transport and wire messages
- [nx-sync crate](/numax/reference/crates/nx-sync/) - operations, CRDTs and deduplication
- [Roadmap](/numax/roadmap/) - planned peer discovery releases
