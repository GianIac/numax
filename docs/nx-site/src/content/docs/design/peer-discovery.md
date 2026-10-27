---
title: Peer Discovery, Membership, and Gossip (draft)
description: How Numax finds peers, tracks membership, disseminates CRDT operations, and repairs missed updates.
---

> **Status: in development.** Endpoint discovery, connection admission, direct
> broadcast, and bounded anti-entropy exist today. Membership, failure detection,
> K-fanout gossip, and the recovery contract below remain planned. The
> [open decisions](#open-decisions) are not implemented or approved designs.

## What this feature does

Numax peers first need to find one another, then decide which connections satisfy
the configured identity policy. Membership tracks which admitted nodes are
believed reachable. Gossip will spread accepted CRDT operations to a subset of
members, while anti-entropy repairs missed updates after loss or a partition.

> Discovery, connection, membership, and synchronized data are separate states. 
> Ordinary local KV writes do not automatically enter this CRDT replication path.

![Peer discovery, admission, membership, gossip, and repair](/numax/diagrams/peer-discovery/overview.svg)

[Mermaid source for this diagram](/numax/diagrams/peer-discovery/overview.mmd)

> The diagram shows the intended feature, including components still in development.

A node can already start with no candidates, discover endpoints later, verify
connections, and exchange CRDT operations with a bounded repair path. 
The intended experience is that a candidate becomes an admitted peer only after the normal
handshake, a missed probe can raise suspicion without proving failure, and an
accepted operation can move across multiple peers without a full broadcast.
If history needed for repair is unavailable, the node must report that condition
instead of claiming convergence.

Local service readiness must remain independent of peer discovery and cluster
convergence. `SyncManager::start()` returns when local services and their loops
are ready, even if there are no candidates or an initial handshake stalls.
An admitted peer may still be recovering data; connection and membership status
must not be mistaken for synchronized state.

### Terms used here

| Term | Meaning |
| --- | --- |
| Candidate | Endpoint suggested by a discovery source or a future manual addition. It may be stale or untrusted. |
| Admitted peer | Remote node whose connection passed the configured handshake and identity policy. Admission is distinct from discovery and from a membership vote. |
| Member | Node represented in the new, weakly consistent membership view. The exact admission and removal transitions remain open. |
| Suspect | Member believed unreachable pending confirmation or refutation; not proof of failure. |
| Accepted operation | A local or remote CRDT operation that has reached the chosen, documented acceptance point. (The current local host API does not yet supply the proposed atomic replay contract) |
| Recoverable gap | Missing accepted history for which the chosen repair source and retention contract can still supply the needed information. |
| Unrecoverable gap | Missing required history beyond that contract. It must become visible as a recovery error. |
| Anti-entropy | Periodic reconciliation independent of the fast gossip path. Today it pulls a bounded operation log; a future replacement requires its own contract. |

## Scope and existing contracts
The following statements describe the inspected v0.1.5 source and its published
contracts.

| Existing contract | Evidence and consequence |
| --- | --- |
| Discovery publishes candidates, not authority | `crates/nx-core/src/discovery.rs` and [Discovery Contract](/numax/design/discovery-contract/). Static, bootstrap gossip, mDNS, DNS-SRV, and file sources feed a bounded coordinator. Provider snapshots, revisions, leases, overflow recovery, and shutdown ownership must remain coherent. |
| Connection identity depends on transport policy | `crates/nx-net/src/node.rs::verify_peer_identity` rejects the local `NodeId`; secure TLS binds the claimed ID to a certificate and checks the peer allowlist. In insecure or non-TLS mode, the reported identity is unverified. A discovered address or cluster name is not proof of trust. |
| Peer health is not SWIM membership | `crates/nx-core/src/sync_manager/peer.rs` tracks per-endpoint reconnection health. Its `Suspect` and `Dead` labels do not constitute a cluster-wide, incarnation-aware membership protocol. |
| Local state and replay metadata have different write stages | `crates/nx-core/src/host_api/crdt.rs` persists local CRDT state and publishes it in memory before enqueueing an `Op`. Some operations generate their identity before state persistence (`ORSetAdd`, `RgaInsert`), others afterward. `sync_manager/replication.rs::broadcast_batch` later persists the seen ID and op-log entry. There is no single atomic local acceptance boundary. |
| Remote apply has a persistence boundary | `crates/nx-core/src/sync_manager/apply.rs` plans remote updates and persists the batch before publishing them in memory. Preserve this error ordering when adding forwarding; verify failure behavior and flush semantics separately. |
| The present repair path is limited | `sync_manager/replication.rs` periodically pulls a bounded op log over active connections. The maximum observed `OpId` is not a causal frontier. Current `OpId` is UUID-based (`crates/nx-sync/src/op.rs`); its lexical order cannot prove that older operations were received. |
| Wire and storage are versioned separately | `crates/nx-net/src/message.rs` defines protocol 5; [Wire Versioning](/numax/design/wire-versioning/) requires an exact version match. [Schema Versioning](/numax/design/schema-versioning/) governs persisted sync namespaces. Both JSON and the `wincode` implementation of the public `Bincode` format matter. |
| Management lists peers today | `GET /api/v1/peers` exists in `crates/nx-api/src/routes.rs`. The proposed POST is not yet a runtime admission API. Control cases belong behind `nx-core/src/control.rs`. |

`nx-store::Store::apply_batch` and `flush` are distinct operations. An atomic
batch application, a completed flush, a local API response, and remote replication
must not be described with the same durability promise. The exact acceptance
semantics are an [open decision](#open-decisions).

## Planned behavior

### Membership and failure detection

![Conceptual peer membership lifecycle](/numax/diagrams/peer-discovery/membership.svg)

[Mermaid source for this diagram](/numax/diagrams/peer-discovery/membership.mmd)

This lifecycle is conceptual: it does not define wire messages or final state
transitions. Candidate, connected, and alive are distinct; suspicion can be
refuted, while rejoining after a failure needs the newer identity evidence.

1. Keep endpoint discovery, identity verification, membership, active transport
   connections, and data recovery separate. A discovered endpoint can initiate a
   connection attempt; it cannot create a trusted member by itself. Membership
   changes must respect the configured authentication mode.
2. Use a bounded, eventually disseminated membership view with explicit join,
   suspect, refute, leave, fail, and rejoin behavior. Define what survives a
   restart and how a newer incarnation supersedes stale information. Different
   nodes may temporarily disagree during a partition; there is no instantaneous
   global membership decision. A relayed claim about another node cannot by
   itself grant that node authenticated admission or override a locally
   verified identity.
3. Treat a missed probe as evidence for suspicion, not proof of a crash. Measure
   false declarations, refuted suspicions, and true-failure detection separately.
   Account for local scheduling delay from CPU-bound WASM or slow storage when
   selecting timeouts. A separate phi-accrual detector is not assumed.
4. Give control traffic its own bounded scheduling opportunity and budget so data
   backpressure cannot indefinitely starve failure detection. Every loop must
   have an owner, cancellation path, bounded queues, and shutdown behavior.

[SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
provides a relevant separation between probing and membership dissemination;
[Lifeguard](https://arxiv.org/abs/1707.00788) addresses false suspicion caused by
a slow local detector. Neither paper selects Numax's transport, trust policy,
timeouts, or wire format. These choices require evidence under Numax workloads.

### Accepted operations, dissemination, and repair

![Planned CRDT gossip and persistence sequence](/numax/diagrams/peer-discovery/replication.svg)

[Mermaid source for this diagram](/numax/diagrams/peer-discovery/replication.mmd)

> The sequence shows desired ordering !!
A node may accept a local CRDT write before it has any peer connection; gossip begins only when eligible peers become available.

1. Establish an atomic local persistence boundary covering the CRDT state,
   operation identity, and replay metadata before the selected local success
   response. The identity must be available for the atomic write; its current
   generation point differs by operation. State exactly whether success means
   batch acceptance or confirmed flush durability, and whether the guarantee
   covers process crashes, graceful restart, or a lost storage device. Do not
   imply a remote acknowledgement unless one is added and specified. Preserve
   the remote apply-before-publish ordering.
2. After a newly accepted remote operation is persisted, make it eligible for
   onward dissemination with its original identity. A duplicate must not create
   a new operation or an unbounded forwarding loop. Storage failure must not
   publish or forward an operation as accepted.
3. Choose a bounded, rotating set of eligible peers. My target target is
   `K = ceil(log2(N) + c)` with a local estimate `N`; calibrate `c`, define
   eligibility, clamp K to configured limits, and handle small clusters.
   Fanout is a dissemination policy, not a proof that every peer has the data.
4. Bound queue entries, retained bytes, message bytes, work per tick, retries,
   and control/data contention. Rate adaptation based on load or RTT must be
   stable and measurable. A dropped send attempt must not discard the only
   recoverable copy of an accepted operation.
5. Keep periodic anti-entropy independent of candidate churn. Make repair
   paginated and byte-bounded, with resumable progress only if the chosen cursor
   actually identifies contiguous received history. Detect gaps and report when
   retention makes them unrecoverable. A new node with no history is a first-class
   case, not an implicit success of the old bounded-log pull.

The recoverability statement is conditional: **no loss of accepted operations within the chosen recovery contract and its declared retention window**. The contract must name the failure model and require at least one surviving,durable source for every operation or equivalent state. Local success alone does not promise redundant copies; loss of the sole durable holder cannot be repaired by fanout or anti-entropy. It cannot be read as arbitrary-duration partition tolerance or complete state transfer. The implementation must expose a clear failure when the contract cannot be met.

### Management and observation

The planned `POST /api/v1/peers` submits a candidate through `RuntimeManagement`
for ordinary validation and eventual admission. Its response must distinguish
candidate acceptance from successful connection, authentication, and membership.
To define its authorization, idempotency, bounds, error mapping, and restart
persistence before fixing the HTTP schema. Once admitted, a manually added peer
must participate in reconnection, detection, and anti-entropy under the same
rules as other admitted peers.

Introspection must distinguish candidates, current membership state, live
connections, replication progress, and unrecoverable gaps. Publish bounded
metrics for probes, suspicion/refutation, fanout, queues, retry, and recovery.
Do not make per-operation or per-remote-address metric labels unbounded.

## Open decisions

I need time to think about it ...

| ID | Decision to make | Viable alternatives and deciding evidence |
| --- | --- | --- |
| D1 | Local acceptance and durability | Is the success point after an atomic batch, or after a confirmed flush? Which failure model does it cover, and is remote durability required before success? How do host API error codes, retries after an unknown outcome, shutdown, and storage failure behave? Require crash injection at each boundary and a restart check of state, `OpId`, and replay metadata. |
| D2 | Missing-history detection and deduplication | Compare a per-origin contiguous sequence (including identity/generation and hole tracking) with idempotent state/delta reconciliation. Current UUID `OpId` plus a seen-ID set detects duplicates only while history remains; a maximum ID is not a frontier. Test out-of-order delivery, delayed duplicates, seen-ID eviction, and replay after restart for each CRDT family. |
| D3 | Recovery horizon | Select an explicit time/byte/operation retention contract and determine how both sides detect that required history expired. State the surviving-source and partition assumptions; decide whether bounded replay suffices or a versioned state-transfer subset is required now. Prove new-node admission, long partitions, and an explicit unrecoverable-gap outcome. |
| D4 | Membership identity and ordering | Define node identity across restart, incarnation/generation storage, state transitions, refutation, tombstone lifetime, leave/rejoin, and stale-message precedence. Existing persisted `NodeId` alone does not settle incarnation ordering. Check restart loops, duplicated control messages, and split-brain merge. |
| D5 | Control transport and trust | Choose how probes, indirect probes, and membership updates use authenticated connections or a separate transport. State behavior in secure TLS and unverified/insecure modes, authorization of relayed claims, payload/queue limits, and replay protection. Prove that endpoint discovery, cluster-name matching, or a third-party membership claim cannot bypass admission. |
| D6 | Detector and timing | Specify probe cadence, suspicion duration, Lifeguard-style local-health adjustment, jitter, and escalation; compare against a separate phi-accrual detector only if the simpler scheme fails the false-positive and latency targets. Calibrate under slow storage, high CPU, 10% loss, and real transport faults. |
| D7 | Fanout and repair scheduling | Define `N`, eligible peers, randomization/rotation, `c`, load/RTT adaptation, recovery contacts, pagination, cursor semantics, and separate control/data budgets. Measure sparse topologies and retained-history gaps; choose defaults from evidence rather than importing them from another protocol. |
| D8 | Runtime peer API and persistence | Decide whether a manual candidate persists across restart, how it interacts with static/dynamic providers and removal, what a successful POST means, and how authorization and rate limits apply. Preserve GET semantics and existing config precedence. |
| D9 | Compatibility and rollout | Choose wire version and schema migration plan only after message and storage shapes are known. Decide mixed-version rejection, in-flight messages, fixture handling, and behavior of v0.1.5 nodes and data. Keep package, wire, and storage versions distinct. Validate both wire formats and a real previous binary. |
| D10 | Data readiness and convergence proof | Define a protocol-specific completion signal and an oracle that accounts for accepted operations as well as durable and materialized state in every CRDT family. Equal final values alone do not prove that a superseded operation was delivered. Keep local readiness, membership, live transport, caught-up state, and unrecoverable repair distinct. |

D1–D3 gate claims of recovery correctness. D4–D6 gate a credible membership
protocol. D7 depends on both; D8–D10 must be resolved before exposing a
publicly usable release.

## Failure scenarios that define the contract

| Scenario | Required observation or unresolved choice |
| --- | --- |
| Crash after state write but before op-log metadata | Current local path can reach this interval. The selected D1 solution must remove the ambiguous accepted state or define a recovery procedure that restores its operation identity. |
| Crash after a batch but before flush or response | The documented D1 durability and retry semantics must match what survives a restart; unknown outcomes cannot be silently turned into acknowledged success. |
| Remote op persisted but forwarding fails | The accepted op remains recoverable by the repair path; a send failure consumes bounded work and does not silently lose the sole source. |
| Sole durable holder fails permanently | No recovery guarantee can be claimed without a surviving replica or equivalent state; D1 and D3 must state whether this case is outside the failure model or requires a remote durability acknowledgement. |
| A newer op arrives before an older one | The receiver must find and request a genuine hole or reconcile safely. Neither `last OpId` nor timestamp order alone establishes completeness. |
| Duplicates arrive after seen-ID eviction | Deduplication and CRDT application must remain safe under the chosen D2/D3 window; beyond it, recovery fails explicitly if safety cannot be established. |
| Partition outlasts retention | Joining partitions report an unrecoverable gap or use an explicitly selected state-transfer path. No false convergence signal. |
| New member joins after useful history expires | Admission and membership visibility are separate from data readiness; the node cannot claim caught-up state without a validated bootstrap/recovery path. |
| Peer connects before finishing repair | Connection and membership may be healthy while data is still incomplete. Report pending or failed recovery until the D10 completion proof succeeds. |
| Healthy member is CPU- or storage-stalled | Suspect and refutation are observed separately from a final failure declaration; control work remains schedulable under load. |
| Node restarts, then an old packet arrives | Incarnation ordering rejects stale membership and replay; an old endpoint cannot overwrite a verified newer identity or generation. |
| Discovery withdraws one candidate | Only that source's contribution disappears. A live admitted connection follows the existing discovery contract; membership changes are governed separately. |
| API candidate is invalid, duplicate, or untrusted | The request is bounded and authenticated; accepted candidacy never claims connection or membership. |
| All nodes shut down at once | State recovery is tested separately from rolling restart. No guarantee inferred from the one-at-a-time scenario. |

> These are test obligations and decision prompts !!

## Validation plan

### Deterministic model and component tests
> TODO

### Compatibility and real transport

For any incompatible control or data message, apply the
[wire versioning procedure](/numax/design/wire-versioning/): test JSON and
Bincode bytes and semantics, update rejection tests, and run a v0.1.5 binary
against the new binary. A mismatch must be rejected before peer registration or
CRDT exchange, without panic or datastore mutation. 
A storage change needs an explicit migration and historic fixture checks under [schema versioning](/numax/design/schema-versioning/); never rewrite fixtures to match new behavior.

Use real sockets and controlled packet loss for at least the transport-sensitive
scenarios. Record whether a failure came from simulation message loss or actual
transport loss. Preserve TLS identity, allowlist, connection limits, management
authentication, and the reserved `__nx/` namespace. Run the relevant ignored
multi-process and mDNS checks rather than assuming `cargo test --workspace`
covers them.

### Documented failure scenarios
> TODO

### Detailed test plan
> TODO
