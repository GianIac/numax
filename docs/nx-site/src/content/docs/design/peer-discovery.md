---
title: Peer Discovery, Membership, and Gossip (draft)
description: How Numax finds peers, tracks membership, disseminates CRDT operations, and repairs missed updates.
---

> **Status: in development.** Endpoint discovery, connection admission, direct broadcast, and bounded anti-entropy exist today.

> **Planning note.** I use this file mainly to plan the development of v0.1.6 as well as I can and to make each decision once and for all. This version is already giving me a hard time, even before I have really started writing code.

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

> The sequence shows the intended ordering, not the current local write path.

A node may accept a local CRDT write before it has any peer connection; gossip
begins only when eligible peers become available.

1. Establish an atomic local persistence boundary covering the CRDT state,
   operation identity, and replay metadata, followed by a confirmed flush
   before the local success response. The identity must be available for the
   atomic write; its current generation point differs by operation. Specify
   the covered crash model and unknown-outcome retry behavior before coding.
   Local success does not imply remote acknowledgement or survival of a lost
   storage device. Preserve the remote apply-before-publish ordering.
2. After a newly accepted remote operation is persisted, make it eligible for
   onward dissemination with its original identity. A duplicate must not create
   a new operation or an unbounded forwarding loop. Storage failure must not
   publish or forward an operation as accepted.
3. Choose a bounded, rotating set of eligible peers. The target is
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

The recoverability statement is conditional: **no loss of accepted operations
within the chosen recovery contract and its declared retention window**. The
contract must name the failure model and require at least one surviving, durable
source for every operation or equivalent state. Local success alone does not
promise redundant copies; loss of the sole durable holder cannot be repaired by
fanout or anti-entropy. The implementation must expose a clear failure when the
contract cannot be met.

### Management and observation

The planned `POST /api/v1/peers` submits a candidate through `RuntimeManagement`
for ordinary validation and eventual admission. Its response confirms candidate
acceptance only; it does not confirm connection, authentication, membership, or
data readiness. Manual candidates will be persisted as a distinct discovery
source and survive restart. Authorization, idempotency, bounds, removal, and
error mapping must be specified before fixing the HTTP schema. Once admitted,
a manually added peer follows the same reconnection, detection, and
anti-entropy rules as other admitted peers.

Introspection must distinguish candidates, current membership state, live
connections, replication progress, and unrecoverable gaps. Publish bounded
metrics for probes, suspicion/refutation, fanout, queues, retry, and recovery.
Do not make per-operation or per-remote-address metric labels unbounded.

## Decisions for v0.1.6

These are approved design directions. None changes the current behavior until
implemented and tested. The [remaining specifications](#remaining-specifications)
are deliberately open; choosing a direction does not select an unmeasured
timeout, retention limit, wire layout, or performance guarantee.

### D1 — Local acceptance and durability

A successful local CRDT write will mean that one atomic storage batch containing
the CRDT state, operation identity, and replay metadata has completed a confirmed
flush. Replication to another node is not part of this local success condition.
The operation identity must be fixed before the batch, including for operations
whose identity is currently generated after state persistence. Materialized and
in-memory state must not be exposed as an accepted update before this boundary.

Multiple waiting writes may share one flush to reduce I/O, but each success
response must wait for a flush that covers its own batch. Grouping changes
latency and throughput, not the meaning of success. Admission to a queue or a
completed batch without flush is insufficient. A flush error or interrupted
response can have an unknown outcome: the caller must not receive success, and
restart/retry behavior must reconcile the durable operation identity rather
than silently creating a second accepted operation. The precise guest error and
retry contract is still to be specified and tested. A confirmed flush protects
the selected local crash model; it is not a promise against destruction of the
storage device or loss of the only durable copy.

#### D1 implementation contract to review

This section makes the accepted D1 direction testable; it does not describe
implemented behavior or choose the remaining wire and storage layouts. Today
`host_api/crdt.rs` writes local CRDT state before sending an operation to the
broadcast queue. For most families it also creates the `OpId` after that state
write. `sync_manager/replication.rs::broadcast_batch` later writes the seen ID
and op-log entry in a separate batch. Its persistence failure drops the queued
send after the local call may have succeeded. The remote path in
`sync_manager/apply.rs` already batches state and replay records before
publishing in memory, but does not flush them. Neither path currently meets the
proposed local acceptance boundary.

For a state-changing local operation, the planned atomic batch must contain its
durable CRDT state, materialized value, unique operation identity, replayable
operation record, and the metadata needed to identify and deduplicate its
logical position. D2's origin, restart generation, and sequence allocation must
eventually be committed at this same boundary; their encoding and allocation
rules are still open. A successful response requires a confirmed flush covering
that batch. A reserved queue slot, a completed batch, or a network send alone
does not satisfy this condition. The in-memory registry and any success result
must follow the flush. The durable replay record remains the recovery source
within the eventual retention contract if the subsequent queue or network send
fails; the queue is only a delivery path.
Any grouped flush must prove that each responding write's batch precedes the
completed flush, including during shutdown and concurrent writes.

The proposed local failure model for the first implementation tests is a
process crash and restart with the same intact datastore. Extend testing to an
OS restart or power interruption only after the filesystem and `sled::flush`
assumptions for that environment are stated and exercised. Device destruction
or loss of the sole durable holder is outside local flush durability. A local
response never certifies remote replication.

| Interruption point | Required response and restart observation |
| --- | --- |
| Before the atomic batch | No success. No new accepted operation or partial CRDT/replay records. |
| Batch application fails | No success. On restart, inspect the whole record set; never treat a partial set as an accepted operation. |
| Batch completes, before or during flush | No success may be returned. The outcome can be unknown: the whole batch may or may not survive. Reconcile its identity before any retry. |
| Flush fails or is interrupted | No success may be returned. A failure does not prove the batch was rolled back; restart must classify the outcome from persisted records. |
| Flush completes, before the caller receives a response | The operation is durable under the selected crash model, but the caller can still see an unknown outcome. A retry must not silently accept the same logical request twice. |
| After the success response | State, operation identity, and replay metadata must all be recoverable together from the intact datastore. Remote receipt remains unproven. |

The current guest API supplies no caller-chosen idempotency token. In
particular, repeating an increment after an interrupted response can create a
second valid operation. Before implementing D1, specify whether the guest API
will expose a stable retry token, offer an outcome lookup, or report an
explicitly non-retryable unknown outcome. Keep the current error codes and ABI
unchanged until that choice and its migration path are reviewed. Do not infer
rollback from `ERR_INTERNAL` or generate a replacement `OpId` for an uncertain
attempt.

All six families need the same acceptance boundary, with family-specific
effects checked separately. GCounter and PNCounter increments must not be
applied twice after an uncertain retry. LwwRegister and LwwMap must preserve the
chosen timestamp and verify the same conflict resolution on replay. ORSet add
must preserve its operation-derived tag, and a remove with no observed tags is
currently a successful no-op with no operation to accept. Rga insert must
preserve its operation-derived element ID; its guest output buffer must be
validated before persistence so a failed output write cannot strand a durable
insert without enqueueing it. Rga delete needs its own replay check. An
operation that leaves a materialized value unchanged may still require a
durable identity and replay record if it is accepted as an operation.

The implementation plan must also resolve two visibility and concurrency
issues. A sled batch can become readable before its flush; delaying only the
in-memory registry update does not by itself prevent direct reads of an
unconfirmed materialized value. Local and remote writers must share a defined
lock order and sequence-allocation owner so concurrent batches cannot reuse a
position, lose an update, or hold a blocking lock across `.await`.

Regression tests must inject failures before the batch, between batch and
flush, during flush, and after flush before response. Reopen the datastore and
compare materialized and durable state, operation identity, seen metadata, op
log, and the eventual origin/generation sequence for each family. Exercise
concurrent writes, grouped flush and shutdown, queue or send failure after
acceptance, and unknown-outcome retries. An injected failure is a component
test; process termination and restart require a separate integration test.
These tests must distinguish an accepted operation from a merely visible
unflushed batch and from a remotely received operation.

### D2 — Identity, missing history, and duplicates

Keep a unique operation ID and add a logical position consisting of origin,
restart generation, and a monotonically increasing per-origin sequence. A
receiver tracks the highest **contiguous** sequence for each origin/generation
and records later arrivals as holes. Receiving sequence 43 after 41 does not
advance the contiguous frontier past 41: sequence 42 must be requested or
reconciled. The maximum UUID or maximum received sequence is not proof of
complete history.

An operation already accepted with the same identity must not be applied or
forwarded again. Sequence allocation and replay metadata belong to D1's atomic
boundary so a crash cannot leave a successful state update without its place in
history. Out-of-order delivery, delayed duplicates, retention expiry, and
restart must be checked for each CRDT family. The exact wire/storage fields,
generation allocation, hole limits, and migration of historical UUID-only
operations remain to be designed before implementation.

### D3 — Recovery and state transfer

Replay missing operations while their required history is retained. When that
history has expired, attempt a minimal, versioned CRDT state transfer whose
completion can be verified for each CRDT family. If neither route can establish
complete state, report an unrecoverable gap; connection or membership must not
be reported as data convergence. This also applies to a new node that joins
after old operations have expired. The retention window, surviving-source
assumptions, transfer format, and completion proof require a separate detailed
contract. State transfer is deliberately brought forward from the later
snapshot work only to the extent required for v0.1.6 recovery.

### D4–D6 — Membership, trust, and failure detection

Membership is an eventually consistent view of admitted node identities, not
the reconnect health of an endpoint. A candidate first passes the normal
handshake and identity policy. A member may then be probed directly and, after
a missed response, indirectly through other admitted members. Missed probes
create suspicion, not immediate proof of failure. A valid newer incarnation
can refute suspicion. Leave is distinct from a timeout; rejoin and restart
must present newer evidence, and stale control messages must not replace it.
Nodes may disagree temporarily during a partition.

Use a SWIM-style membership protocol with Lifeguard-style adjustment for the
local detector's scheduling health. A CPU-bound guest or slow storage can delay
our own probes; suspicion timing must account for this instead of treating
every local delay as a remote fault. This may increase true-failure detection
latency, so measure both false declarations and real-failure latency. Do not
add an independent phi-accrual detector without evidence that the selected
model misses the release targets.

Use the existing connection transport and its configured identity policy for
control messages, with bounded control queues and a scheduling budget separate
from data gossip. In insecure or unverified modes, a claimed identity remains
unverified; a relayed membership claim never grants authenticated admission.
Probe cadence, suspicion duration, incarnation persistence, tombstone lifetime,
indirect probe limits, and replay protection remain to be specified and tested.

### D7 — Gossip and repair scheduling

Introduce K-fanout only after D1–D3 establish accepted operations and recovery.
Estimate `N` from eligible members, clamp `K = ceil(log2(N) + c)` to available
peers and configured budgets, and handle empty and small clusters explicitly.
Rotate a bounded neighbor set and retain useful recovery contacts so sparse
topologies and healed partitions can reconnect. A newly accepted remote op
becomes eligible for onward forwarding only after persistence, with its original
identity; duplicate delivery must not create a forwarding cycle.

Periodic anti-entropy must be paginated and byte-bounded. A failed or dropped
send attempt does not delete the durable source needed for repair. Calibrate
`c`, timing, jitter, load/RTT adaptation, queue sizes, and retry budgets from
deterministic and real-transport measurements rather than fixing arbitrary
defaults in this RFC.

### D8–D10 — Management, compatibility, and readiness

Persist manual candidates as their own discovery source across restart. A
successful `POST /api/v1/peers` means candidate acceptance only. Validation,
authentication, admission, reconnection, membership, and recovery retain their
normal boundaries. Preserve GET behavior and configuration precedence; specify
duplicate submission, removal, authorization, rate limits, and error mapping
before implementing the API.

Version incompatible wire and storage changes explicitly. Reject incompatible
v0.1.5 peers before registration or CRDT exchange, and migrate historical data
without rewriting fixtures. Verify JSON and the `wincode` implementation of
public Bincode with a real previous binary. Package, wire, and schema versions
remain independent.

Expose data readiness separately from local service readiness, membership, and
active connection. Convergence evidence must account for accepted operation
identities and for durable and materialized CRDT state across all six families;
equal final user-visible values alone are insufficient. An unrecoverable gap is
an explicit failed recovery state, never a successful convergence result.

### Remaining specifications

Before coding the affected behavior, document and review: the local crash and
unknown-outcome retry contract; per-origin generation allocation and bounded
hole tracking; retention limits and state-transfer completeness; membership
state ordering and control message authentication; fanout defaults and budgets;
manual candidate removal; and the wire/schema migration procedure. These
details are implementation gates, not permissions to silently choose a protocol
or weaken the release criteria.

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

Each scenario is a test obligation. Its expected result is conditional on the
approved acceptance and recovery contracts above; a passing network send alone
does not satisfy a durability or convergence check.

## Validation plan

### Deterministic model and component tests

Use one membership and dissemination state machine in production and in the
simulation, with an injected clock and transport, a seeded PRNG, and stable
event/candidate ordering. Record seed, topology, workload, payload sizes,
latency, retention, and loss model so a failing run is reproducible. Unit and
component tests cover state transitions, incarnation precedence, suspected
members who refute suspicion, leave/rejoin, duplicate and stale control
messages, queue limits, and cancellation. Simulation message loss is labeled
separately from real packet loss.

For local persistence, inject a failure before the atomic batch, after batch
acceptance but before flush, during flush, and after flush but before the caller
receives a response. Restart from the resulting store and compare the
materialized value, durable CRDT state, operation identity, per-origin
sequence, seen metadata, and op log. A successful write must have all required
records; a failed or unknown-outcome write must not be reported as successful.
Test grouped flush under concurrent writes and shutdown so every successful
response is covered by a confirmed flush. Measure its latency and throughput
against an ungrouped baseline without relaxing the success condition.

For recovery, deliver operations out of order, omit one sequence, replay it
late, and replay duplicates after seen-ID eviction and restart. Verify the
contiguous frontier, bounded hole tracking, and absence of duplicate effects
or forwarding loops. Repeat with GCounter, PNCounter, LwwRegister, LwwMap,
ORSet, and Rga: their operation semantics differ. Test replay inside retention,
expiry beyond retention, a new node with no useful history, successful
versioned state transfer, and explicit failure when no verified transfer is
possible. Compare accepted identities and durable/materialized state, not
only final values.

Run the primary 50-node scenario with a declared 10% simulated loss model and
a 60-second partition into two groups of 25. Stop new writes at rejoin and
measure whether convergence completes within 30 seconds without loss of
accepted operations under the stated recovery contract. Run a separate case
with writes continuing during repair. Include sparse topologies, varying
payloads and load, and a partition longer than retention; the latter must
produce a clear recovery failure if state transfer cannot close the gap.
Publish seeds, configuration, workload, observations, and resource use. A
100% rolling restart means restarting nodes one at a time with recovery
between restarts; test a simultaneous full shutdown separately.

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

Exercise direct and indirect probing, suspicion, refutation, stale messages,
and reconnect under real transport loss. Include CPU-bound WASM execution and
slow storage to measure local scheduling delay before interpreting probe
timeouts. A defined one-hour nominal run reports false failure declarations,
refuted suspicions, true-failure detection latency, queue pressure, and resource
use. Zero false declarations in that run is an observation for that workload,
not a universal guarantee. Test authenticated control under TLS and the
documented limits of insecure/unverified mode. A relayed claim must never
bypass normal admission.

Exercise `POST /api/v1/peers` with invalid, duplicate, untrusted, and valid
candidates. Verify that acceptance does not imply connection or data readiness,
that manual candidates survive restart, and that an admitted manual peer joins
reconnection, detection, and repair. Keep the current GET contract intact.

### Documented failure scenarios

The table above is the failure matrix. Each row needs a reproducible test or
simulation case with the injected fault, expected state transition, durable
records, externally visible status, and whether recovery eventually succeeds
or fails explicitly. In particular, a failed send after local acceptance must
leave a recoverable durable source; an expired history window must not be
reported as synchronized merely because the peers are connected.

### Detailed test plan

Implement the tests in dependency order: D1 persistence and crash/restart;
D2 out-of-order delivery and duplicate handling; D3 replay and state transfer;
D4–D6 membership and transport under load; D7 fanout and repair; D8–D10 API,
compatibility, and convergence evidence. Use component tests near the owning
module, integration tests for storage/network boundaries, and end-to-end tests
for each CRDT family. The [roadmap](/numax/roadmap/) closing criterion remains
unchanged; report missing environmental dependencies and tests not run rather
than treating planned tests as completed evidence.
