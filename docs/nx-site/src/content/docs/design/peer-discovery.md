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
| Accepted operation | A CRDT operation that has reached its documented persistence boundary. Local writes now batch state and replay metadata and wait for flush; remote apply still has a separate durability contract to review. |
| Recoverable gap | Missing accepted history for which the chosen repair source and retention contract can still supply the needed information. |
| Unrecoverable gap | Missing required history beyond that contract. It must become visible as a recovery error. |
| Anti-entropy | Periodic reconciliation independent of the fast gossip path. Today it pulls a bounded operation log; a future replacement requires its own contract. |

## Scope and existing contracts
The following statements describe the inspected checkout and its published
contracts.

| Existing contract | Evidence and consequence |
| --- | --- |
| Discovery publishes candidates, not authority | `crates/nx-core/src/discovery.rs` and [Discovery Contract](/numax/design/discovery-contract/). Static, bootstrap gossip, mDNS, DNS-SRV, and file sources feed a bounded coordinator. Provider snapshots, revisions, leases, overflow recovery, and shutdown ownership must remain coherent. |
| Connection identity depends on transport policy | `crates/nx-net/src/node.rs::verify_peer_identity` rejects the local `NodeId`; secure TLS binds the claimed ID to a certificate and checks the peer allowlist. In insecure or non-TLS mode, the reported identity is unverified. A discovered address or cluster name is not proof of trust. |
| Peer health is not SWIM membership | `crates/nx-core/src/sync_manager/peer.rs` tracks per-endpoint reconnection health. Its `Suspect` and `Dead` labels do not constitute a cluster-wide, incarnation-aware membership protocol. |
| Local acceptance is flush-confirmed | Local CRDT mutations persist state, `OpId`, seen metadata, and replay in one atomic batch. Success follows the covering flush and does not imply remote receipt. |
| Remote apply has a persistence boundary | `crates/nx-core/src/sync_manager/apply.rs` plans remote updates and persists the batch before publishing them in memory. Preserve this error ordering when adding forwarding; verify failure behavior and flush semantics separately. |
| The present repair path is limited | `sync_manager/replication.rs` periodically pulls a bounded op log over active connections. The maximum observed `OpId` is not a causal frontier. Current `OpId` is UUID-based (`crates/nx-sync/src/op.rs`); its lexical order cannot prove that older operations were received. |
| Wire and storage are versioned separately | `crates/nx-net/src/message.rs` defines protocol 5; [Wire Versioning](/numax/design/wire-versioning/) requires an exact version match. [Schema Versioning](/numax/design/schema-versioning/) governs persisted sync namespaces. Both JSON and the `wincode` implementation of the public `Bincode` format matter. |
| Management lists peers today | `GET /api/v1/peers` exists in `crates/nx-api/src/routes.rs`. The proposed POST is not yet a runtime admission API. Control cases belong behind `nx-core/src/control.rs`. |

`nx-store::Store::apply_batch` and `flush` are distinct operations. An atomic
batch application, a completed flush, a local API response, and remote replication
must not be described with the same durability promise. Local acceptance is
implemented as specified in [D1](#d1--local-acceptance-and-durability), within
its documented verification limits. The current remote persistence boundary and
absence of replication acknowledgements are specified in
[Acknowledgement semantics](#acknowledgement-semantics).

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

> The sequence combines the implemented local acceptance boundary with planned
> gossip, remote forwarding, and recovery behavior. It is not a trace of the
> complete current replication path.

A node may accept a local CRDT write before it has any peer connection; gossip
begins only when eligible peers become available.

1. Preserve the implemented atomic local persistence boundary covering the
   CRDT state, materialized value, operation identity, seen metadata, and replay
   record, followed by a confirmed flush before the local success response.
   This boundary covers all six families; [D1](#d1--local-acceptance-and-durability)
   specifies the retry contract, verified process-crash model, and evidence limits.
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

These are approved design directions. Only the implemented and tested portions
change current behavior. The [remaining specifications](#remaining-specifications)
are deliberately open; choosing a direction does not select an unmeasured
timeout, retention limit, wire layout, or performance guarantee.

### D1 — Local acceptance and durability

**Status:** implemented and verified for the current local CRDT mutation APIs.

D1 defines when Numax may report a local CRDT mutation as successful and what
the current delivery path confirms. It does not guarantee remote durability or
cluster convergence, or define retention or state transfer.

#### Acceptance contract

A local mutation is accepted only after one atomic batch containing all of the
following has completed a successful flush:

- durable CRDT state;
- materialized value;
- operation identity (`OpId`);
- seen-operation metadata;
- replayable operation record.

The acceptance sequence is:

1. Validate the request and reserve delivery capacity.
2. Create the operation identity.
3. Apply the complete storage batch.
4. Flush the batch.
5. Publish the new state in memory.
6. Enqueue the accepted operation for dissemination.
7. Return success to the caller.

A completed batch, queue insertion, network send, or peer connection does not
by itself constitute local acceptance.

The current implementation performs one synchronous flush per local batch
under the store write lease. A future grouped flush is valid only if every
successful mutation is covered by the completed flush.

#### Result semantics

| Caller result | Guaranteed outcome |
| --- | --- |
| Success | The complete local batch was flushed. Remote receipt is not guaranteed. |
| Rejected during validation | No operation was accepted. |
| Flush or internal error | The outcome is uncertain; the batch may have survived. |
| Interrupted response | The outcome is uncertain even if the operation was accepted. |

After a flush failure, the store rejects further writes until it is reopened.
Reopening recovers any complete surviving operation from its durable state and
replay metadata.

#### Retry contract

The current guest ABI has no caller-provided idempotency token and cannot
identify the outcome of a lost request.

Mutating wrappers do not retry automatically after an internal error,
unexpected result, or interrupted response. Repeating the call explicitly
creates a new operation identity and may apply the logical mutation again.

RGA insertion is the only exception: the SDK may retry `ERR_BUF_TOO_SMALL`
because output capacity and bounds are validated before persistence.

![Local CRDT write outcomes and guest retry boundary](/numax/diagrams/peer-discovery/local-crdt-outcomes.svg)

[Mermaid source for this diagram](/numax/diagrams/peer-discovery/local-crdt-outcomes.mmd)

#### CRDT-specific rules

| Mutation | Additional rule |
| --- | --- |
| PNCounter increment/decrement | Each direction creates a distinct replayable operation. |
| LWW Register set | Losing writes retain their operation identity and replay record. |
| LWW Map set/remove | Removes persist tombstones; unchanged visible state does not remove the replay record. |
| ORSet add | The operation identity is also the add tag. |
| ORSet remove | Exactly the observed tags are removed. No observed tags produces a successful no-op without an operation. |
| RGA insert | The operation identity is also the element identity. Output validation precedes persistence. |
| RGA delete | The delete has its own identity and persists the tombstone. |

#### Replication boundary

The broadcast queue and network transport are delivery mechanisms, not
durability or replication acknowledgements.

Failure after local acceptance cannot roll back the flushed replay record.
Recovery and anti-entropy may use that record according to the retention
contract defined by D2 and D3.

Remote application has a separate persistence boundary and is not covered by
this local acceptance contract.

#### Acknowledgement semantics

The current API confirms local acceptance only. It does not wait for an
application-level acknowledgement from another node, require a replica count
or quorum, or expose a per-operation remote durability result.

| Event | What it establishes | What it does not establish |
| --- | --- | --- |
| Storage `apply_batch()` returns successfully | The batch was applied atomically in the local store. | A flush covering the batch has completed. This alone is not local CRDT acceptance. |
| Storage `flush()` covering the local batch returns successfully | The local durability boundary required by D1 has been reached, within the failure model below. | Any remote receipt, application, or durable copy. |
| Local mutation returns success | The D1 acceptance sequence completed for the new operation. ORSet remove without observed tags is a successful no-op. | Remote replication or cluster convergence. |
| Network send returns successfully | The local transport write and stream flush completed without a reported error. | The peer application received, applied, or flushed the operation. A stream flush is not a storage flush. |
| Receiver emits `NodeEvent::OpsReceived` | The decoded batch reached the network event path. | Successful CRDT application or persistence; this event is internal, not an acknowledgement to the sender. |
| Remote apply returns successfully for new operations | The receiver applied the state, seen metadata, and replay records in an atomic storage batch before publishing the updates in memory. | A covering storage flush has completed; the remote apply path does not explicitly wait for one before publication. |

`Node::broadcast_ops()` can return success with no connected peers. With peers,
its success concerns local transport writes to the selected connections, not
remote application. A failed broadcast can also have delivered operations to
some peers; it does not undo local acceptance or prove that nobody received
the batch. The caller must not repeat a logical mutation merely to retry its
delivery. The existing replay record retains its original operation identity
and remains subject to the bounded retention and recovery contract.

`PushOpsAck { received_count }` exists in the public message and wire formats,
but the current receiver does not send it and the sender does not wait for it.
An incoming `PushOpsAck` falls through the read loop's unhandled-message branch;
its count is not used to establish receipt, application, durability, or a
contiguous recovery frontier. `HelloAck` confirms the handshake, and `Pong`
answers a ping; neither confirms CRDT replication.

These statements describe the existing implementation, not a new ACK exchange.
Any future remote durability acknowledgement requires a separate contract for
the operations covered, the receiver's persistence boundary, duplicates,
failures, timeouts, and wire compatibility. A count alone must not be treated
as proof of which operations are durable.

For example, node A can return local success while node B is disconnected. A
then holds the flushed operation, but B has no confirmed copy. Permanent loss
of A's storage before another durable copy exists is outside the local
acceptance guarantee.

Implementation evidence: `nx-store/src/store.rs` separates batch and flush;
`nx-core/src/sync_manager/storage.rs` flushes local batches but not remote
batches before returning; `nx-core/src/sync_manager/apply.rs` publishes after
the remote batch; `nx-net/src/node.rs` implements transport sends and dispatches
received messages; `nx-net/src/message.rs` retains the unused ACK variant.
These paths are relative to `crates/`.

#### Verified failure model

Verification covers the current local mutation variants, concurrent writes,
shutdown, uncertain guest responses, flush errors, and process termination at
these boundaries:

| Termination point | Restart requirement |
| --- | --- |
| Before the batch | The new operation is absent. |
| After the batch, before `flush()` | Records are either all present or all absent. |
| After `flush()` returns | State, identity, seen metadata, and replay record are present. |

The datastore is reopened intact after process termination. The `BeforeFlush`
checkpoint stops before calling `sled::flush()`; it does not interrupt sled
while the flush is executing.

This evidence does not cover OS reboot, power loss, storage-device loss,
interruption inside `sled::flush()`, remote acknowledgement, or transactional
isolation for direct internal reads of reserved storage keys.

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

Before coding the affected behavior, document and review: an expanded OS or
power-interruption crash model; per-origin generation allocation and bounded
hole tracking; retention limits and state-transfer completeness; membership
state ordering and control message authentication; fanout defaults and budgets;
manual candidate removal; and the wire/schema migration procedure. These
details are implementation gates, not permissions to silently choose a protocol
or weaken the release criteria.

## Failure scenarios that define the contract

| Scenario | Required observation or unresolved choice |
| --- | --- |
| Crash between local state and replay persistence | D1 now writes both in one atomic batch for all six families; recovery must not expose a surviving new state without its operation identity and replay metadata. The batch may survive before flush without having been accepted. |
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
The current per-batch flush has concurrent-write and shutdown tests. If grouped
flush is introduced, verify that every successful response is covered by a
confirmed flush and measure latency and throughput against the current
per-batch baseline without relaxing the success condition.

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
