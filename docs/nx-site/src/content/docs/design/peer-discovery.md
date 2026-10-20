---
title: Peer Discovery, Membership, and Gossip (draft)
description: How Numax finds peers, tracks membership, disseminates CRDT operations, and repairs missed updates.
---

This RFC defines the replication contracts and proposed membership architecture
for v0.1.6. Endpoint discovery, connection admission, direct broadcast,
sequenced anti-entropy and bounded CRDT baseline recovery are implemented.

## System boundaries

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
Admission requires the normal handshake. The proposed membership detector
uses suspicion before declaring failure; planned gossip forwarding preserves
the identity of accepted operations across multiple peers.
If history needed for repair is unavailable, the node must report that condition
instead of claiming convergence.

Local service readiness is independent of peer discovery and cluster
convergence. `SyncManager::start()` returns when local services and their loops
are ready, even if there are no candidates or an initial handshake stalls.
An admitted peer may still be recovering data; connection and membership status
must not be mistaken for synchronized state.

### Terminology

| Term | Meaning |
| --- | --- |
| Candidate | Endpoint suggested by a discovery source or a future manual addition. It may be stale or untrusted. |
| Admitted peer | Remote node whose connection passed the configured handshake and identity policy. Admission is distinct from discovery and from a membership vote. |
| Member | Node represented in the new, weakly consistent membership view. Internal transitions are specified below; runtime admission and lifecycle integration remain pending. |
| Suspect | Member believed unreachable pending confirmation or refutation; not proof of failure. |
| Accepted operation | A CRDT operation that has reached its documented persistence boundary. Local writes batch state and replay metadata and wait for flush; remote apply has the separate persistence boundary specified below. |
| Recoverable gap | Missing accepted history for which the chosen repair source and retention contract can still supply the needed information. |
| Unrecoverable gap | Missing required history beyond that contract. It must become visible as a recovery error. |
| Anti-entropy | Periodic reconciliation independent of the fast gossip path. It repairs retained history by replay or requests a verified baseline within the D3 recovery limits. |

## Current architecture

The following contracts apply to the implemented discovery and replication path.

| Responsibility | Contract |
| --- | --- |
| Discovery publishes candidates, not authority | `crates/nx-core/src/discovery.rs` and [Discovery Contract](/numax/design/discovery-contract/). Static, bootstrap gossip, mDNS, DNS-SRV, and file sources feed a bounded coordinator. Provider snapshots, revisions, leases, overflow recovery, and shutdown ownership must remain coherent. |
| Connection identity depends on transport policy | `crates/nx-net/src/node.rs::verify_peer_identity` rejects the local `NodeId`; secure TLS binds the claimed ID to a certificate and checks the peer allowlist. In insecure or non-TLS mode, the reported identity is unverified. A discovered address or cluster name is not proof of trust. |
| Peer health is not SWIM membership | `crates/nx-core/src/sync_manager/peer.rs` tracks per-endpoint reconnection health. Its `Suspect` and `Dead` labels do not constitute a cluster-wide, incarnation-aware membership protocol. |
| Local acceptance is flush-confirmed | Local CRDT mutations persist state, `OpId`, seen metadata, and replay in one atomic batch. Success follows the covering flush and does not imply remote receipt. |
| Remote apply has a persistence boundary | `crates/nx-core/src/sync_manager/apply.rs` plans remote updates and persists the batch before publishing them in memory. The remote batch precedes in-memory publication and does not explicitly await a covering flush. |
| Data recovery | `sync_manager/replication.rs` exchanges paginated per-origin summaries and bounded replay ranges over active connections. Durable contiguous prefixes and sparse receipt metadata suppress delayed duplicates independently of replay retention. Unavailable history triggers bounded baseline recovery; UUID order and advertised maxima are not receipt frontiers. |
| Wire and storage are versioned separately | `crates/nx-net/src/message.rs` defines protocol 9; [Wire Versioning](/numax/design/wire-versioning/) requires an exact version match. [Schema Versioning](/numax/design/schema-versioning/) governs persisted sync namespaces. Both JSON and the `wincode` implementation of the public `Bincode` format matter. |
| Management | `GET /api/v1/peers` exists in `crates/nx-api/src/routes.rs`. The proposed POST is not yet a runtime admission API. Control cases belong behind `nx-core/src/control.rs`. |

`nx-store::Store::apply_batch` and `flush` are distinct operations. An atomic
batch application, a completed flush, a local API response, and remote replication
have distinct durability guarantees. Local acceptance follows
[D1](#d1--local-acceptance-and-durability) and its bounded failure model. The current remote persistence boundary and
absence of replication acknowledgements are specified in
[Acknowledgement semantics](#acknowledgement-semantics).

## Planned behavior

### Membership and failure detection

The proposed [D4 state machine](#d4-member-identity-and-ordering) separates
candidate admission, membership state and connection status. Suspicion can be
refuted; restart and leave/rejoin obey explicit epoch/incarnation ordering.

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

## Replication contracts and membership design

D1–D3 specify the implemented acceptance, sequencing and recovery contracts.
D4–D10 describe the proposed membership, dissemination and management design.
The [remaining specifications](#remaining-specifications) identify parameters
that have no fixed value or performance guarantee.

### D1 — Local acceptance and durability

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
| Receiver emits `NodeEvent::HistoryReceived` | The decoded batch reached the network event path. | Successful CRDT application or persistence; this event is internal, not an acknowledgement to the sender. |
| Remote apply returns successfully for new operations | The receiver applied the state, seen metadata, and replay records in an atomic storage batch before publishing the updates in memory. | A covering storage flush has completed; the remote apply path does not explicitly wait for one before publication. |

`Node::broadcast_history()` can return success with no connected peers. With peers,
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

Implementation boundaries: `nx-store/src/store.rs` separates batch and flush;
`nx-core/src/sync_manager/storage.rs` flushes local batches but not remote
batches before returning; `nx-core/src/sync_manager/apply.rs` publishes after
the remote batch; `nx-net/src/node.rs` implements transport sends and dispatches
received messages; `nx-net/src/message.rs` retains the unused ACK variant.
These paths are relative to `crates/`.

<span id="verified-failure-model"></span>

#### Process-crash model

The local acceptance contract assumes process termination with an intact
datastore that can be reopened. Concurrent writes, shutdown, interrupted guest
responses and flush errors obey the same acceptance boundary. Restart behavior
is defined at the following points:

| Termination point | Restart requirement |
| --- | --- |
| Before the batch | The new operation is absent. |
| After the batch, before `flush()` | Records are either all present or all absent. |
| After `flush()` returns | State, identity, seen metadata, and replay record are present. |

The datastore is reopened intact after process termination. The `BeforeFlush`
checkpoint stops before calling `sled::flush()`; it does not interrupt sled
while the flush is executing.

OS reboot, power loss, storage-device loss, interruption inside
`sled::flush()`, remote acknowledgement and transactional isolation for direct
internal reads of reserved storage keys are outside this failure model.

### D2 — Identity, missing history, and duplicates

D2 defines durable operation positions and receiver coverage. Recovery beyond
retained history follows [D3](#d3--recovery-and-state-transfer).
The UUID `OpId` remains the unique operation identity, with a logical position
`(origin, generation, sequence)` carried by the replication envelope.
The sequence is monotonic per origin across process restarts; generation
identifies the origin's process incarnation and increases durably at each start.
The sequence does not reset at a restart. This keeps one contiguous history per
origin and avoids needing a separate end-of-generation marker to prove where an
earlier generation stopped.

#### Allocation and persistence

- Persist the origin identity with its local generation metadata. On startup,
  increment the generation and confirm its flush before accepting writes. If
  the store is already bound to a different `NodeId`, fail closed rather than
  silently restarting the sequence under another identity.
- Allocate the next per-origin sequence while holding the existing local
  mutation locks. Persist the sequence high-water mark, operation identity,
  CRDT state, materialized value, seen metadata, and replay record in D1's
  atomic batch. A failed or uncertain write must never cause the same sequence
  to be assigned to a different surviving operation after restart.
- For a newly received sequenced operation, persist its position and receiver
  tracking state in the same atomic batch as the CRDT update, seen metadata,
  and replay record, before publishing in memory. Do not describe this remote
  batch as flush-confirmed unless its path also waits for a flush.
- Keep `OpId` separate from the logical position: UUID identity remains useful
  for legacy identification and diagnostics, while the position establishes ordering
  and duplicate safety after the bounded `seen-op` cache evicts an ID.

The store binding, generation, local sequence, received frontier, and sparse
positions use versioned schema records. Protocol 8 carries the replication
envelopes; the legacy public `Op` and message encodings remain unchanged.
See [Wire Versioning](/numax/design/wire-versioning/) and
[Schema Versioning](/numax/design/schema-versioning/).

#### Receiver behavior

For each origin, track the highest **contiguous** sequence and a bounded set of
received positions beyond it. Starting from sequence 0, if sequence 2 arrives
first, retain it as sparse and leave the contiguous frontier at 0. Receiving
sequence 1 then advances the frontier through 2. Never infer completeness from
the largest received sequence or UUID.

For a position still retained in the sparse set, the same generation and
`OpId` identify a duplicate. A different generation or `OpId` at that retained
`(origin, sequence)` is an identity conflict: reject it without changing state.
At or below the contiguous frontier, suppress the operation without applying
or forwarding it. **A compacted frontier proves that a position is covered,
not which UUID or generation occupied every historical position.** Do not
promise exact conflict detection after those identities have been discarded.

Bound both the number of sparse positions per origin and their distance from
the contiguous frontier. **If either limit is exceeded, reject the new
operation before changing CRDT state and expose the unresolved gap**; silently
evicting sparse positions would make later duplicate handling unsafe.

Also bound the total number of tracked origins, aggregate sparse entries, and
resident and persisted tracking bytes. Keep durable contiguous frontiers when
replay entries expire or members leave; removing a cache entry must not erase
the protection against old operations. If an origin or metadata update cannot
fit the global budget, reject it before applying its operations and expose the
resource limit. Never evict a durable frontier merely to admit another origin.
Paging durable metadata into memory is allowed only if duplicate checks still
consult its authoritative record before application.

The implementation uses the following hard budgets; these are correctness and
resource bounds, not performance targets:

| Resource | Limit |
| --- | --- |
| Tracked origins | 1,024 |
| Origin / operation-ID UTF-8 bytes | 256 / 128 |
| Sparse positions per origin / total | 256 / 8,192 |
| Serialized receipt keys and values | 4 MiB total |
| Sequence distance from contiguous prefix | 65,536 |
| Summary origins per page | 16 |
| Requested sequences per replay page | 128 |
| Replay operation-array JSON size | 8 MiB |

The replay budget accommodates a maximum 1 MiB guest value in JSON byte-array
form. Before accepting a new operation, reserve 1 KiB of that budget for
framing/metadata; an operation that cannot fit is rejected before its state
batch. Replay-position sidecars are bounded by the configured op-log retention
count and identity-length limits, independently of the receipt-record budget.
Sequence and generation exhaustion and corrupt tracking metadata fail closed.
Sequence 0 is a frontier sentinel, not a valid operation.

#### Missing-operation detection and bounded progress

A gap between received operations does not reveal a lost final operation. For
example, after receiving sequence 1, a node cannot know that accepted sequence
2 was lost if the producer stops writing. **Periodically exchange bounded,
paginated per-origin history summaries independently of new writes.** A summary
advertises the highest sequence the sender knows to exist. These summaries
report observed history, including when sent by the origin; they do not certify
flush-confirmed acceptance or a durable remote copy. D1 remains the contract
for the local success response.

Compare the advertised high-water mark with the receiver's contiguous frontier
and sparse positions to identify missing ranges, including a missing suffix.
Never advance the receiver's frontier from an advertisement. It advances only
when the corresponding operations and tracking metadata have been persisted
together, or when the D3 state-transfer contract establishes an equivalent
baseline. A matching frontier certifies coverage only through that advertised
sequence, not global or instantaneous cluster convergence. Without contact
with a source or relay that knows about a lost operation, its existence cannot
be detected by this exchange.

Budget summary entries and bytes, range requests, replay bytes, and retries
per cycle; the receiver retains a per-connection origin cursor and periodically
requests the next summary page, so each tracked origin is eventually visited
while communication continues. Recovery therefore does not depend on the
producer choosing the same anti-entropy interval. Numeric maxima do not demonstrate
that every requested replay record remains available. A source must check the
actual requested interval and report unavailable history instead of treating
an empty or partial response as complete recovery.

After persisting a valid replay page, request at most one next missing range
from that source immediately. Do not wait for another periodic summary to
continue an already known gap. Every request and response still obeys the
range and byte budgets; unavailable history is retried through the periodic
exchange rather than an immediate retry loop.
Request a bounded interval starting at the first missing sequence, including
already received sparse positions within that interval. Receiver deduplication
suppresses those duplicates. This closes multiple scattered gaps per page
instead of requiring a separate network round trip for each missing position.
Keep at most one outstanding range per origin and 16 per connection. Repeated
pushes or summaries must not duplicate an in-flight request. A valid replay
prefix or unavailable response releases its matching request; failed sends
release it immediately. Missing responses become eligible for retry after the
configured socket timeout. Disconnect removes the connection's pending requests.
Each range attempt has a fresh request ID. Accept replies only from the pending
source and origin, with that ID, an unexpired deadline, and a matching interval.
Persist unavailable evidence with its source, interval and request ID; remove
it when that interval is covered, not only when all later gaps are closed.
Likewise, allow only one outstanding summary request per connection until its
reply or socket-timeout deadline; short polling intervals must not build a
backlog of identical summaries while a peer is busy.

When a far-ahead operation exceeds the gap-distance or sparse-entry budget,
leave it unapplied, record the bounded recovery target, and request the earliest
missing range first. Bounded replay pages must allow that frontier to advance
without requiring acceptance of the rejected far-ahead operation first. If
retained history cannot close the gap, expose a recovery failure or the need
for D3 state transfer. Global origin-budget exhaustion likewise remains an
explicit resource failure; it must not be presented as synchronized state.

Continue to apply valid out-of-order CRDT operations after their identity and
bounded tracking metadata can be persisted. This matches the current
operation-based apply path, but it is conditional on tests proving convergence
for all six families, especially ORSet observed removals and RGA parent/delete
reordering. If a family does not preserve its specified merge behavior under
these orders, buffer the causally dependent operation for that family or revise
the operation contract before enabling sequenced replication.

#### Legacy history and recovery boundary

Existing operations contain only UUID identity, and the old replay log is
bounded. Do not manufacture historical origin sequences from UUID order or
local op-log order. Preserve those records as explicitly unsequenced legacy
operations for migration and inspection, together with the existing CRDT state.
**Do not automatically apply or forward legacy operation replay over the new
replication path.** An old ID missing from the bounded seen cache does not
prove that its operation was never applied; replaying a counter increment can
otherwise apply its effect twice. Reject unsupported legacy replay before
changing state and expose the need for baseline recovery.

Remote recovery of legacy state requires D3's verified state-transfer and
baseline contract, or an explicit unsupported/unrecoverable result while that
path is unavailable. A new sequenced frontier never certifies coverage of
legacy history. The implementation must preserve pre-migration local values
and prevent automatic legacy op-log replay; merely tagging an operation as
legacy is insufficient. Keep historical fixtures unchanged and test migration
using copied fixtures. Sources advertise `baseline_required` for pre-sequence
state, and receivers preserve that condition independently of their new
contiguous prefixes.

The current protocol requires exact version equality. A wire version that
requires sequence metadata therefore rejects v0.1.5 peers before CRDT exchange;
deployment and tests must account for the resulting mixed-version disconnects.
Do not claim lossless recovery of legacy history or across expired retention.

#### D2 acceptance tests

The deterministic receipt tracker is shared by production and tests. Its
acceptance criteria are:

1. In-order delivery and sparse arrivals, such as `2, 1`, with frontier and
   sparse set asserted after each event.
2. Delayed duplicates before and after the contiguous frontier advances,
   including after restart and after bounded `seen-op` eviction; verify no
   second CRDT effect or duplicate forwarding.
3. Conflicting operation IDs or generations at a retained sparse position;
   verify rejection and unchanged durable and materialized state. After prefix
   compaction and identity eviction, verify suppression without promising exact
   historical conflict classification.
4. Per-origin sparse-count and gap-distance limits, total origin and aggregate
   metadata budgets; verify bounded resources, rejected application, durable
   frontier preservation, and a visible unresolved-gap or resource failure.
5. Crashes before and after the atomic local and remote persistence batches;
   after restart, state, position, frontier, sparse entries, seen metadata, and
   replay records must agree.
6. Every CRDT family under reordered dependencies, followed by convergence to
   the same state as a valid in-order delivery.
7. UUID-only historical fixture migration; preserve existing values, suppress
   automatic legacy replay, reject unsupported incoming legacy operations even
   after old seen-ID eviction, and never advance a frontier from legacy history.
8. Lose the last accepted operation, stop new writes, and verify detection
   through periodic summaries followed by recovery when history is available.
   Cover a relayed summary and temporary disconnection without claiming unseen
   operations are detectable before a knowledgeable source becomes reachable.
9. Reject a far-ahead operation at the gap limit, then close the gap through
   bounded replay pages from the earliest missing sequence. Test unavailable
   intervals and partial responses without false completion.
10. Paginate summaries over more origins than fit one cycle; verify eventual
    visitation and that maxima never advance the receiver's contiguous frontier.

#### Replication interfaces

The private protocol-8 `History` envelope carries sequenced pushes, paginated
summaries, correlated replay ranges and baseline recovery messages. Peers with
other wire versions are rejected before admission. The `sequences` and op-log
tables use schema 3; the [schema contract](/numax/design/schema-versioning/)
defines their records and offline migration steps.

`SyncHandle::replication_progress()` exposes contiguous and known-high
sequences, sparse counts, unavailable-range evidence, missing legacy baselines,
unconfirmed baseline flush and source-scoped recovery failures. None of these
source-specific observations proves permanent cluster-wide loss. Resource and
operational errors are reported through sync-error metrics and logs.

UUID, retained position and payload are validated together under the storage
write lease, including after seen-ID eviction. Invalid pushes do not persist
recovery targets; typed recoverable tracking limits can leave a target for
bounded repair. Global receipt budgets apply to the final atomic batch, rather
than origin-order-dependent intermediate totals.

The implementation and verification entry points are:

| Responsibility | Module |
| --- | --- |
| Receipt transitions and compacted duplicate suppression | `sync_manager/sequence.rs` |
| Per-origin allocation, receipt records and budgets | `sync_manager/storage/sequence.rs` |
| Request correlation, replay validation and anti-entropy | `sync_manager/replication.rs` |
| Manager integration, restart and transport scenarios | `sync_manager/tests/sequencing.rs` |
| Baseline state validation and atomic installation | `sync_manager/baseline.rs` |
| Recovery across expiry, legacy state, restart and storage failure | `sync_manager/tests/recovery.rs` |

#### Wire/schema evolution and historical-data verification

Wire and storage versions are independent. Protocol 8 requires exact peer
version equality; op-log and sequence schema 3 reject writers that cannot
preserve baseline and receipt metadata. The compatibility matrix and offline
upgrade procedure are specified in
[Schema Versioning](/numax/design/schema-versioning/#upgrade-historical-state-and-downgrade-contract).
[Wire Versioning](/numax/design/wire-versioning/#compatibility-verification-and-deployment)
defines admission, message encoding and deployment boundaries.

Historical fixtures are immutable input to migration and compatibility tests:

| Fixture | Required preservation |
| --- | --- |
| `sync_manager/fixtures/v0_1_0/` | Unversioned records, CRDT state and original operation payloads |
| `sync_manager/fixtures/v0_1_5/` | Six CRDT families, tombstones, schema-1 headers, seen IDs, UUID-only replay and an unrelated application key |

Fixtures use their release encoders and pinned source revisions. Migration
preserves historical payloads, supports checkpoint resumption, and rejects
corrupted records without advancing that record's checkpoint. Historical
UUID-only log indices never become per-origin sequence positions. A legacy
baseline requirement remains independent of new sequence coverage.

Frozen protocol-7 frame hashes and older codec references protect their
respective layouts; they do not grant mixed-version admission. Current codec
and transport verification covers JSON and Bincode, safe rejection against the
v0.1.5 binary, explicit migration and downgrade protection. Reproducible commands
are listed under
[compatibility verification](/numax/design/schema-versioning/#compatibility-verification).

### D3 — Recovery and state transfer

Replay missing operations while their required history is retained. When that
history has expired, attempt a minimal, versioned CRDT state transfer whose
completion can be verified for each CRDT family. If neither route can establish
complete state, report an unrecoverable gap; connection or membership must not
be reported as data convergence. This also applies to a new node that joins
after old operations have expired. The retention window, surviving-source
assumptions, transfer format, and completion proof are defined by the bounded
contract below. State transfer is deliberately brought forward from the later
snapshot work only to the extent required for v0.1.6 recovery.

#### Selected bounded recovery contract

D3 retains the last `max(1, op_log_limit)` operations accepted at a node, across
all origins (default 10,000), in insertion order. This is a count window, not a
time guarantee: unrelated writes, imported operations and a smaller restart
configuration can evict history. Recovery by replay requires a surviving,
reachable, admitted source to retain every requested position; different sources
may supply different ranges. Retained min/max positions alone do not prove a
hole-free interval. The source answers from actual records and their durable
positions. Receipt coverage survives replay eviction.

When replay is unavailable, or a legacy baseline is advertised but absent, the
receiver requests baseline format **1** over wire protocol **8**. The minimal
subset is one complete, byte-bounded CRDT baseline in one frame: no application
KV, executable modules, store identity, remote durability acknowledgements or
general-purpose backup API. The source captures all six registries and their
origin cut under the existing lock order and store write lease, and confirms a
covering flush before sending. It must have no known history holes, sparse
receipts, missing legacy baselines or unconfirmed baseline flush.

The payload is JSON independently of the negotiated outer JSON/Bincode codec.
It contains sorted unique origins with contiguous sequences and legacy provenance,
and sorted unique `(family, key)` records containing full internal state JSON.
LWW winners, ORSet add/remove tags and RGA elements/tombstones are preserved.
BLAKE3 covers the exact payload bytes. Limits are 1 MiB encoded payload, 4,096
records, 4,096 key bytes and the existing 1,024-origin budget. RGA baseline
materialization additionally requires at most 4,096 elements and depth 128;
cycles, mismatched element IDs and overflowing counter totals are rejected.
The outer JSON byte-array encoding fits the existing default wire limit;
smaller configured transport limits may refuse the transfer. There is no
staging or partial publication: an interrupted frame installs nothing.

The receiver only replaces state with a **dominating complete cut**: every
already applied contiguous/sparse position and every installed legacy baseline
must be represented, the local allocator must remain unchanged, and the offered
state must include every existing key and dominate its internal state under that
family's merge. A summary maximum is never sufficient. The stricter cut rule is
necessary for additive operation-based counters: max-merging partial receipt
sets then replaying the remaining increments is unsafe. Concurrent local writes
or incomparable replicas can therefore refuse a baseline; D3 does not promise
arbitrary divergent-replica reconciliation beyond retained history.

Full state, materialized values, legacy coverage and receipt frontiers are
installed in one atomic batch, then flushed. Imported prefixes suppress delayed
duplicates even when their UUIDs no longer appear in the bounded seen/op logs.
Existing replay records remain usable; new replay starts after the imported cut.
Known maxima beyond that cut remain pending. A batch rejection publishes nothing.
A flush failure is an uncertain outcome: the full batch may survive and is kept
coherent in memory, but a persisted confirmation marker prevents successful
export or reporting confirmed baseline completion until the store is reopened
and a restart flush confirms it. A failed flush fences further writes; an
in-process retry cannot clear that fence.

Sequence and op-log schema **3** fence runtimes lacking these baseline semantics.
Version 2 records migrate without changing their payloads; the store's own
pre-sequence baseline provenance is established at initialization. Advertised
remote provenance never establishes receipt. Historical fixtures remain unchanged.

Requests share the existing per-peer/origin request budget, correlation and
deadlines. Baseline requests capture a local fingerprint of receipt coverage.
A refusal after another source changed that coverage is ignored even if the
attempt has not expired; stored failures are invalidated on later coverage or
known-history changes. This fingerprint adds no wire or storage fields. Stale or
unsolicited replies are ignored. Unsupported versions,
incomplete sources, budget exhaustion, invalid state and coverage conflicts are
explicit contract failures. Storage/metadata errors keep their original cause
and reach sync-error logs/metrics rather than being converted to those
rejections. An operational export failure sends no misleading baseline refusal;
the request may expire and retry. `replication_progress()` distinguishes missing
legacy state, unconfirmed flush and the last **source-scoped** recovery failure. That failure
is not a proof of global permanent loss: another source or later repair can still
succeed, and periodic anti-entropy keeps trying. No disconnected/unknown peer is
assumed to have lost its state. Data convergence requires no known missing ranges,
no missing legacy baseline and no unconfirmed flush, relative to the discovered
cut; it is not a certificate that every cluster operation has been discovered.

This contract requires a surviving complete source within these budgets after
replay expires. If none can supply one, recovery fails explicitly under this
contract. In particular, a new node joining an old cluster must obtain replay or
a validated baseline before being considered caught up. Permanent loss of the
sole durable holder remains outside the guarantee. Larger paginated snapshots,
arbitrary incomparable-cut merging and unrestricted lossless recovery remain
future work.

### D4–D6 — Membership, trust, and failure detection

The membership contract separates the member view (D4), evidence from probing
and suspicion (D5), and control transport, admission and resource ownership
(D6). The development runtime integrates the shared D4/D5 state machine,
durable metadata and isolated protocol-9 control RPCs through `SyncManager`.
Membership does not
change D1–D3 acceptance, receipt tracking or baseline recovery.

#### Internal core and integration boundary

`crates/nx-core/src/membership/` is private. Its domain core is independent of
async I/O; its runtime adapter owns the concrete network and storage boundary:

| Module | Responsibility |
| --- | --- |
| `state.rs` | Admitted-member view, epoch/incarnation ordering, refutation, terminal-state persistence requests, floor restoration and coordination of probe results |
| `probe.rs` | Correlated direct/indirect attempts, dispatch confirmation, ACK/NACK validation, cancellation and randomized round-robin target selection |
| `suspicion.rs` | Validated timing inputs, saturating local health and logarithmically shortened suspicion deadlines |
| `schema.rs` | Version-1 membership metadata, identity binding and monotonic rejection-floor merge |
| `storage.rs` | Synchronous datastore adapter, bounded restoration, exclusive write lease, atomic batch and covering-flush confirmation |
| `runtime.rs` | Owned Tokio lifecycle, per-epoch admitted sessions, dispatch notifications, helper service, reconciliation, clock/PRNG adapter and serial blocking storage worker |

The caller supplies nondecreasing logical time as elapsed `Duration` values,
admitted identities with nonzero session identifiers, persistence results and
bounded random-index choices. The same event and choice trace produces the
same actions without sleeping or opening sockets. The core performs no identity
authentication: admission is a trusted adapter input, not a replacement for the
normal handshake. The runtime seeds SplitMix64 from `getrandom` and supplies
bounded choices to the same deterministic schedule. Its monotonic logical clock
cancels active attempts and excludes a locally missed interval over 100 ms
rather than classifying that scheduling delay as remote failure. This can
increase detection latency; it is not an arbitrary-stall latency guarantee.

The core emits bounded actions instead of performing network or storage work:
publish an update, retain an attributed suspicion for dissemination, prioritize
a buddy notification, send a correlated probe, or persist an epoch/rejection
floor. A covering-flush confirmation is required before publishing initial
`Alive`, `Dead` or `Left`. Pure state-machine tests can inject confirmations;
the storage adapter produces them only after a real covering flush succeeds.
An uncertain startup flush fences announcements. An accepted owner leave closes its epoch
in memory even if floor persistence fails; its terminal publication still
requires successful persistence. Superseded persistence tokens are ignored.

| Core limit | Accepted range |
| --- | --- |
| Remote member records, including retained terminal records | 1 through 4,096 configured slots |
| Identity length | 1 through 256 bytes |
| Simultaneous probe attempts | 1 through 64 |
| Helpers per attempt | 0 through 16 |
| Additional suspicion confirmations | 0 through 64 |
| Maximum local health score | 0 through 1,024 |

These are constructor validation ceilings. Runtime defaults are a two-second
probe period, 500 ms direct timeout, one active attempt, three helpers, 5–15 s
suspicion bounds, three additional confirmations and health capped at eight.
Direct timeout and period scale with health as specified below. These defaults
are exercised by local tests, not calibrated as universal production settings.
Terminal
records are not evicted or compacted by the core. Admitting a new identity when
slots are exhausted fails explicitly. Probe action batches are bounded by the
configured attempt and helper limits; byte budgets for encoded messages remain
part of D6. Local leave cancels owned attempts and prevents further probing.

`SyncManager` allocates and flushes the local epoch before announcing readiness,
owns the control adapter and attempts durable leave before closing the network.
Discovery may be empty; startup does not wait for a successful remote exchange.
The adapter serves direct/indirect probes, prioritizes buddy updates, coalesces
membership records and reconciles four-record pages. It retains known endpoints
for bounded revalidation after disconnection or partition. CRDT replication and
endpoint reconnection retain their existing responsibilities and topology.

The runtime has at most eight outgoing control jobs, one reconciliation start
per 250 ms, 64 unvalidated endpoint hints and 4,096 member/session records.
Transport has independent control ingress/egress limits, a 16 KiB payload limit
and a separate bounded pre-handshake pool. Terminal I/O uses one blocking worker
and at most 64 queued actions; the core continues serving probes while a flush
is pending. Only a matching successful completion publishes the terminal state.
Storage failure fences the service and remains observable in its internal
snapshot and shutdown result. A stale queued write is skipped; a superseded
in-flight write cannot authorize a stale publication.

Shutdown bounds leave dissemination to two seconds and adapter completion to
five seconds. Cancellation aborts the owned adapter and network jobs. A running
filesystem operation cannot be forcibly interrupted and may complete afterward,
but it cannot publish through the canceled adapter. Package versions and CRDT
schemas are unchanged; the wire version is now 9. Public management expansion,
K-fanout and release-scale acceptance remain separate work.

##### Durable membership metadata

The adapter owns the separate versioned `__nx/membership/` namespace described
in [Schema Versioning](/numax/design/schema-versioning/#membership-metadata).
It does not change CRDT table versions, replication generations or operation
sequences. Existing application and sync records remain untouched.

Startup reads bounded pages under a datastore write lease, validates the schema
and local identity binding, restores remote terminal floors without sessions,
and allocates the next checked local epoch. The schema header and bound epoch
commit in one batch. A covering flush completes before initial `Alive` is
returned; it also covers any surviving metadata from an interrupted write.
Restored `Dead` and `Left` records are not announced as fresh observations and
are not eligible for probing. Fresh admission is required to acquire authority.

For terminal transitions, the adapter checks that the token, member, pair and
status still match pending work before touching storage. It verifies that the
stored local epoch still belongs to this instance, merges the persisted floor,
writes and flushes it, then confirms the transition. Duplicate or superseded
actions produce no storage write or publication. The floor retains both the
greatest terminal pair and the greatest closed epoch, even when a later `Dead`
supersedes an earlier `Left`. No floor expires automatically.

Storage, metadata and capacity failures fence that core, including outstanding
confirmation callbacks. A failed batch does not imply successful persistence;
a failed flush has an uncertain outcome and requires reopening the datastore.
Recovery retains every surviving floor and completes a new covering flush
before announcing the next epoch. Errors preserve the original storage cause.
The adapter does not restore live sessions, timers, health, PRNG state or probe
attempts, and does not recreate authenticated peers from disk.

Tests pause real flushes to check publication ordering and terminate child
processes before/after batch and flush, across initial creation, restart and
leave. This establishes the bounded process-crash contract, not power-loss or
storage-device guarantees. Runtime integration must still own exactly one
membership lifecycle for the local identity and schedule blocking storage work
without stalling control traffic; the synchronous adapter starts no tasks.

#### Membership in plain language

Discovery supplies possible addresses. Admission checks who is at an address
under the configured identity policy. Membership is each node's changing view
of which admitted nodes appear reachable. It is not a central attendance list:
two nodes can temporarily disagree, especially during a network partition.

Suppose Alice, Bob and Carol are admitted nodes. Alice asks Bob to reply to a
small liveness check, called a probe. If Bob does not reply in time, Alice asks
Carol to try as well. Carol might reach Bob even when Alice cannot. If neither
path returns a valid reply before the attempt expires, Alice suspects Bob; she
does not yet declare him dead. Bob might simply be busy, or Alice might be slow
at processing replies. With no eligible Carol, Alice has no independent helper;
she must not invent an extra confirmation.

Suspicion gives Bob a chance to answer the accusation. He announces a newer
version of his own liveness record, called a refutation. If the suspicion expires
without a valid refutation, Alice can mark Bob `Dead`. This means "my detector
judges this member unreachable", not "the machine is permanently gone". It does
not revoke Bob's identity, delete his CRDT data or prove that any data is lost.
Likewise, `Alive` means neither that all nodes agree nor that Bob is caught up.

Lifeguard adds a second question: "Could the observer be slow?" A node that misses
expected feedback becomes more cautious. Separate reports from other members
can shorten a suspicion, while repeated copies of one report cannot. A node
also prioritizes telling Bob that he is suspected, so Bob can refute promptly.

#### Protocol basis and adaptation boundary

[SWIM: Scalable Weakly-consistent Infection-style Process Group Membership
Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf),
sections 4.1–4.3, supplies membership dissemination, suspicion/incarnation
ordering and randomized round-robin probing. A failed direct probe is followed
by bounded indirect probes; missing replies start suspicion before failure is
declared. Updates are disseminated with bounded retransmission and piggybacking.

[Lifeguard: Local Health Awareness for More Accurate Failure
Detection](https://arxiv.org/html/1707.00788), section IV, supplies three
extensions selected for this design:

| Extension | Required behavior |
| --- | --- |
| Local Health Aware Probe (§IV-A) | A saturating local-health multiplier adapts probe interval and timeout. Feedback includes successful/failed probes, self-refutation and missing expected indirect negative replies. |
| Local Health Aware Suspicion (§IV-B) | Suspicion starts with a conservative timeout. Distinct corroborating reporters shorten it toward a configured minimum; repeated reports by the same member do not count again. |
| Buddy System (§IV-C) | Prioritize notifying a suspected member about its suspicion so that it can refute promptly. |

Lifeguard extends SWIM; it is not a separate membership service. Its reported
results and memberlist defaults do not establish Numax's performance or timing
parameters. Numax must validate its TCP/TLS adaptation, scheduler and storage
interactions. A partial implementation must identify omitted extensions and
must not be described as the complete selected Lifeguard design. No independent
phi-accrual detector is selected.

The [Lifeguard paper, revision 2](https://arxiv.org/html/1707.00788v2),
sections IV-A through IV-C, is the algorithmic reference. The official
[memberlist implementation at revision 4e3e17f](https://github.com/hashicorp/memberlist/tree/4e3e17fc20e38ac48982ab6f65fec243fd139ef9)
is an implementation comparison, not a second interchangeable specification.
In that revision, `probeNode` scales the overall probe interval but deliberately
does not scale the direct UDP wait; failed-probe health feedback also depends
on the number of missing NACKs. Those choices differ from a literal reading of
the paper's rules below. Numax uses TCP/TLS, not that UDP-plus-TCP-fallback path;
any departure must be named and tested, rather than silently combining both.

#### D4: member identity and ordering

![Proposed membership state machine with owner refutation and epoch-closed leave](/numax/diagrams/peer-discovery/membership.svg)

[Mermaid source for the state machine](/numax/diagrams/peer-discovery/membership.mmd)

The diagram shows the proposed local view of one member. A valid newer epoch
supersedes an older epoch from any state after normal admission. Refutation
within an epoch is allowed only while that epoch is open. Stale updates,
responses from old sessions and superseded timers leave the current state
unchanged; the transition table below defines the guards.

Each member record identifies a `NodeId`, a membership start epoch, a
self-issued incarnation, a state and bounded endpoint metadata. Connection
instances, local observation timestamps and reporter identities are separate
metadata. Wall-clock timestamps do not order membership updates.

The proposed start epoch is a monotonic, durable counter bound to the local
`NodeId`, advanced and flush-confirmed before that start announces membership.
The incarnation starts at zero in a new epoch and increases when the member
refutes a suspicion about itself. Only that member issues its newer epoch or
incarnation; observers issue suspicion/failure evidence about an existing one.
A self-refutation uses an incarnation greater than both the local value and the
valid suspicion it refutes. Overflow, identity mismatch, corrupt metadata or an
uncertain epoch flush prevents announcements under a new epoch.

This is membership metadata, separate from the replication allocator. D2's
`(origin, generation, sequence)` remains unchanged; a suspicion must not advance
its generation or consume a CRDT sequence. Storage keys, versioning and
initialization are specified in [Schema Versioning](/numax/design/schema-versioning/#membership-metadata). Losing or restoring
an older identity store does not guarantee a newer epoch; automatic recovery of
that situation is outside this contract.

For example, `(epoch 7, incarnation 2)` is newer than `(7, 1)`, but older than
`(8, 0)`. The epoch distinguishes durable starts; the incarnation distinguishes
self-refutations within one start. These are logical counters, not wall-clock
times. If Bob leaves epoch 7, even `(7, 100)` cannot reopen it: he must rejoin
with a newer durable epoch. This durable epoch and closed-leave design is a
Numax extension, not a persistence guarantee inherited from SWIM or Lifeguard.

Compare epochs first, incarnations second. For the same pair, failure evidence
has precedence `Dead > Suspect > Alive`. A valid owner-issued `Left` closes
that epoch and cannot be undone by another update within it. Rejoin after leave
requires a newer durable epoch. This leave rule is a Numax design choice, not
a guarantee supplied by the papers.

| Event | Required transition |
| --- | --- |
| Previously unknown endpoint or relayed identity | Keep as a bounded candidate/claim until the normal identity/admission policy succeeds; do not grant membership authority from the suggestion alone. |
| Admitted member announces a fresh epoch | Establish `Alive` for that epoch; invalidate pending work for earlier epochs. |
| Direct and indirect probe attempt fails for an `Alive` pair | Enter `Suspect`, capture the pair and start its suspicion deadline. |
| Valid `Suspect` for an existing pair | Merge suspicion; duplicate delivery does not restart its deadline. |
| Member receives valid suspicion or failure about itself | If not leaving, issue a newer self-incarnation and disseminate `Alive`. |
| Valid newer owner-issued `Alive` | Replace the older suspicion/failure, unless the epoch was explicitly closed by `Left`. |
| Suspicion deadline expires | Declare `Dead` only if its epoch/incarnation and timer still match the current suspicion. |
| Valid owner-issued leave | Enter `Left`, cancel work for that epoch and disseminate leave within the shutdown budget. |
| Stale pair, old connection response or canceled timer | Ignore without changing membership or completing newer work. |

A matching probe acknowledgement proves reachability for its attempt. It does
not by itself override a gossiped suspicion/death at the same incarnation;
refutation requires the owner's newer `Alive`. `Dead` is a local failure
judgment, not proof of permanent failure, identity revocation or data loss.
A connected or `Alive` member may still require D3 recovery.

Detailed dead/left records may expire only after a configured retention period.
Compaction must retain a durable rejection floor, including epoch-closure
information for leave, so that replay cannot resurrect an old member after
restart. Persist the required rejection floor before publishing a terminal
transition or discarding its detailed record. Storage/flush failure is local
degradation and must not be presented as successful durable compaction. These
floors and all member records count toward explicit global budgets. Exhaustion rejects new membership metadata visibly rather than
silently evicting replay protection. No finite TTL alone makes unbounded delayed
messages safe. The version-1 storage representation is specified in [Schema Versioning](/numax/design/schema-versioning/#membership-metadata).
Any future floor-removal policy requires an explicit contract; unlimited identity churn is not promised.

#### D5: probes, suspicion and local scheduling

![Proposed SWIM direct and indirect probes with Lifeguard suspicion and refutation](/numax/diagrams/peer-discovery/membership-probes.svg)

[Mermaid source for the probe sequence](/numax/diagrams/peer-discovery/membership-probes.mmd)

The sequence shows one helper from a bounded set. With no eligible helper,
the indirect branch has no sends and proceeds under the same attempt deadline.
Buddy notification is prioritized on an available control path, not guaranteed
delivery to an unreachable target. Local scheduling/send failure is handled as
local degradation, outside the failed-remote-probe branches shown here.
The exchange illustrates the model, not literal protocol-9 frame contents.

Probe eligible known members through a seeded, randomized round-robin schedule;
a member must not starve merely because repeated random choices select others.
There is no probe for an empty view. A one-peer view cannot request independent
helpers. Helpers exclude the target, self, duplicates and ineligible members.
Indirect requests do not recursively create more indirect requests.

Each attempt carries a fresh identifier, initiator and target identities,
target epoch/incarnation, connection/session correlation and a deadline.
Responses must match the active attempt and authorized response path. Late,
duplicate or unsolicited acknowledgements and negative replies cannot complete
another attempt or become independent confirmations. An indirect negative
reply reports that helper's observation; it is not proof of target death.

Bound the Lifeguard health multiplier and adapted intervals. Suspicion counts
distinct reporter `NodeId`s, not messages, endpoints or connections. Its timer
is measured from the first accepted suspicion using a monotonic clock;
corroboration can shorten the deadline within its configured range, but cannot
restart it indefinitely. Record enqueue, send and event-processing delay
separately from reply latency. Failure to schedule or send locally is observable
local degradation, not a completed remote probe. Arbitrary scheduler or network
stalls have no guaranteed detection latency.

The production adapter now records test-only tick lateness in a bounded sample
buffer. On macOS, one CPU-bound WASM run collected 80 tick samples over 1.745 s:
p50 15.821 ms, p95 21.376 ms and max 22.265 ms; one probe succeeded, none
failed, and all observed members remained `Alive`. A separate paused-membership-
flush run collected 40 samples: p50 1.323 ms, p95 1.984 ms and max 2.050 ms;
there were no failed probes or false state transitions, and a control probe
received an ACK before the flush was released. These are single-run local
observations, not calibrated thresholds, percentiles across repetitions, or a
combined WASM-plus-storage stress result. Reproduce them with:

```sh
cargo test -p nx-core production_adapter_makes_progress_with_cpu_bound_wasm -- --nocapture
cargo test -p nx-core production_adapter_serves_probes_during_flush_and_fences_storage_errors -- --nocapture
```

##### Paper-derived health and suspicion rules

In Lifeguard section IV-A, local health is a saturating counter `h` between zero
and a configured maximum `S`. Zero is the healthiest score; a larger score
means the local detector should be more cautious.

| Feedback in the paper | Change to `h` |
| --- | --- |
| Successful direct or indirect probe | -1 |
| Failed probe | +1 |
| Refuting a suspicion about self | +1 |
| Probe with a missing expected negative reply (NACK) | +1 |

The paper scales both the probe interval and timeout:

```text
probe_interval(h) = base_interval * (h + 1)
probe_timeout(h)  = base_timeout  * (h + 1)
```

A NACK means an enlisted helper did not obtain the target's reply within its
helper wait. It is not a death certificate or an independent suspicion vote.
Missing an expected NACK can indicate slow local processing, which is why it
affects the observer's health score. A NACK must not close an attempt that can
still receive a timely ACK through another authorized path.

The core applies feedback once when an attempt completes. A failed attempt
adds one point, plus one further point if any dispatched helper lacks a NACK;
the extra point is per attempt, not per missing message. Success subtracts one
point even if an earlier helper sent a NACK. Self-refutation adds one point
only when it advances the local incarnation. Duplicate replies or repeated
delivery of an older self-accusation add nothing.

Direct/overall deadlines start at confirmed direct dispatch, using the health
score captured when the attempt was created. An unsent direct request has a
bounded queue lifetime. If selected helper work was not dispatched before the
overall expiry, the result is local degradation, not remote failure. With no
eligible helpers, a dispatched direct attempt can expire normally. Only
dispatched helpers create NACK expectations. Explicit send failure cancels the
attempt without changing remote state or health. The helper-side handler and
its negative-reply deadline remain transport-integration requirements.

In section IV-B, suspicion begins conservatively and shortens with distinct
additional reporters. Rename the paper's `K` to `q` here to avoid confusing it
with D7's data fanout. For `q > 0`:

```text
c = min(distinct_additional_reporters, q)
duration(c) = max(Tmin, Tmax - (Tmax - Tmin) * log(c + 1) / log(q + 1))
deadline(c) = first_suspicion_time + duration(c)
```

The initial reporter is not an additional confirmation. Repeated messages from
that reporter, or forwarding its report through different connections, do not
increase `c`. Independent here means distinct reporting members, not proof of
statistically independent observations. Reporter attribution must survive relay.
The paper also re-gossips the first `q` independent confirmations; coalescing
must preserve the distinct evidence needed to shorten the deadline.

For an illustrative `Tmin = 2 s`, `Tmax = 6 s`, `q = 3`, the durations are
`6 s`, `4 s`, approximately `2.83 s`, and `2 s` for zero through three additional
reporters. These are durations from the original start, not fresh waits. If the
first confirmation arrives one second after suspicion began, the new deadline
is four seconds after that start: three seconds remain. If a shortened deadline
is already past, process its expiry with the normal state/pair guards.
These example numbers are not proposed Numax defaults.

![Illustrative suspicion timeline: a distinct reporter shortens the deadline, a duplicate does not, and refutation or guarded expiry determines the outcome](/numax/diagrams/peer-discovery/suspicion-timeline.svg)

[Mermaid source for the suspicion timeline](/numax/diagrams/peer-discovery/suspicion-timeline.mmd)

The two branches are alternative outcomes of the same example, not consecutive
steps. Both use `Tmin = 2 s`, `Tmax = 6 s`, `q = 3` and the original start at
`t = 0`. The refutation branch assumes a valid newer owner `Alive` is processed
before expiry while the epoch is still open. The expiry branch requires the
suspicion to remain current and the required rejection floor to be persisted
before publishing `Dead`.

At suspicion creation, the core freezes `q` to the lesser of the configured
threshold and the number of other currently eligible reporters, excluding the
target and initial reporter. With `q = 0`, it uses `Tmin` directly without
evaluating the logarithmic expression. Memberlist also uses the minimum for
a zero effective threshold, but its rule for reducing that threshold differs.
This small-cluster policy is an explicit Numax adaptation.

Section IV-C's Buddy System prioritizes carrying the suspicion when probing the
suspected member, including a helper's probe. In plain terms: tell Bob about the
accusation at the next usable opportunity instead of letting unrelated gossip
occupy that space. This improves his opportunity to refute; it cannot guarantee
delivery across a broken connection or partition.

The paper reports `base_interval = 1 s`, `base_timeout = 500 ms`, `S = 8` and
`q = 3` as reference settings. Its section V-C relates suspicion bounds to
group size using `Tmin = alpha * log10(n) * probe_interval` and
`Tmax = beta * Tmin`. These are experimental reference settings and relations,
not validated Numax defaults. The core takes explicit minimum/maximum durations
and freezes them at suspicion creation. A future runtime configuration must
define any size-derived policy, including the estimate of `n` and whether its
interval is base or health-adjusted. Membership churn does not restart timers.

Target selection begins from stable identity order and applies Fisher-Yates
shuffling through injected, range-checked random choices. Waiting members keep
their positions across churn; new candidates for that cycle join behind them.
Empty membership produces no probe. Calls before the next allowed probe time
produce no send, and missed periods do not cause catch-up bursts. Helpers are
distinct eligible `Alive` members, excluding self and target.

Probe replies at or after the overall deadline cannot succeed. Corroboration
that shortens a suspicion past the current time immediately requests the
guarded terminal persistence action. Otherwise, equal-time events follow the
caller's explicit event order; timer callbacks always recheck the current pair
and token. Duration arithmetic is checked for overflow; logarithmic durations
use `f64` and Rust's checked conversion to nanosecond-resolution `Duration`,
with a minimum clamp and exact `Tmin` at the threshold. Identical traces are
reproducible on the same execution platform; cross-platform bit-exact floating
point results are not an additional guarantee.

Runtime validation must calibrate timing against CPU-bound WASM, synchronous
storage contention and data backpressure, reporting the tradeoff between false
declarations and true-failure detection latency.

#### D6: transport, authentication and dissemination

Reuse the existing TCP/TLS transport and identity policy as the starting design.
Secure TLS authenticates the immediate peer under the configured allowlist;
insecure/unverified mode retains its documented limits. A relayed claim is not
an end-to-end proof of the subject's identity. Unknown subjects require ordinary
admission before becoming locally eligible members. Known-member claims are
attributed to their admitted reporters under a cooperative, crash-fault model;
Byzantine resistance or transferable owner signatures are not promised.
Direct owner-issued updates must match the admitted session identity. Relayed
updates retain their reporter attribution; transport authentication does not
prove that the subject originated them. The authority matrix below specifies
the enforced separation. Owner `Alive` is revalidated directly; a known-pair
`Left` may use the explicit cooperative relay rule below.

##### Control authority matrix

The matrix describes the protocol-9 adapter's contract. The internal core assumes trusted
admission/session inputs; it is not a network authentication boundary.

| Control input | Required source and correlation | Authority and limits |
| --- | --- | --- |
| Admission and initial owner `Alive` | Normal handshake and identity policy, followed by valid owner metadata | An endpoint, certificate connection or relayed claim alone does not create a member. Secure TLS binds the immediate peer identity; plain/insecure modes remain unverified. |
| Direct owner `Alive` or refutation | Subject equals the admitted immediate peer; current session and valid epoch/incarnation ordering | Only the owner's newer pair refutes its suspicion/death while the epoch is open. A closed epoch cannot be reopened by a greater incarnation. |
| Direct owner `Left` | Subject equals the admitted immediate peer; exact current pair and session | Close the epoch. Publish the terminal transition only after its rejection floor has been flushed. Neither a probe ACK nor a reporter can act as an owner leave. |
| Direct `Suspect` or `Dead` evidence | Admitted reporter on its current session, distinct from the subject; current known subject pair | Attribute the observation to that reporter. Self-accusations reach the local refutation rule; remote terminal publication still requires floor persistence. This is crash-fault evidence, not proof of permanent failure. |
| Forwarded suspicion evidence | Original reporter and subject pair remain separate from the immediate forwarder | Ingress accepts suspicion only when its reporter equals the immediate admitted sender. Other forwarded suspicions are ignored rather than counted as new evidence. |
| Relayed owner `Alive` | Admitted immediate reporter, separately identified owner and bounded endpoint hint | Revalidate the owner directly before admission or refutation. A forwarded record never creates an owner session. |
| Relayed owner `Left` | Admitted immediate reporter and exact already-known subject pair | The explicit cooperative relay handler may close that subject epoch after floor persistence. It authenticates the relay, not owner origination; unknown subjects and local-self leave claims are rejected. |
| Direct probe and indirect request | Admitted initiator; bounded known target and captured request context | Direct ACK requires the exact local owner pair. A helper directly probes only a known matching target, without recursive indirect requests; missing or ineligible targets produce no positive evidence. |
| Direct or indirect ACK | Active attempt; exact target pair; current captured target session or a dispatched helper's captured identity/session | Complete only that attempt. A helper ACK is an attributed helper response, not transferable owner authentication. Success does not override same-pair suspicion/death or establish CRDT recovery. |
| Helper NACK | Active attempt and matching dispatched helper/session, received before expiry | Negative helper feedback, not a suspicion confirmation or death verdict. A timely authorized ACK may still succeed afterward. |
| Buddy notification or reconciliation record | Admitted sender; preserve subject, owner/reporter attribution and applicable ordering guards | Delivery priority, a snapshot or a different envelope grants no extra authority. Unknown subjects remain bounded claims until admitted. |

For example, if Alice's secure connection is with Carol and Carol forwards
`Alive(Bob)`, the certificate proves Carol's immediate identity, not Bob's
origination of that update. A cooperative crash-fault relay policy may trust
such claims once specified; this is not Byzantine resistance or an owner
signature. Only known-pair `Left` has cooperative relay authority. Every RPC
authenticates its immediate peer afresh; the adapter assigns a nonzero core
session to that admitted identity's epoch. A newer epoch replaces that session
and invalidates captured probes; request IDs isolate individual TCP exchanges.
This logical session is not a transferable identity proof or a data connection.

The adapter commits a session only after the core confirms that exact owner
version and session as `Alive`. Restored terminal floors have no live session:
a rejected owner does not populate the session map, so a later valid refutation
can still be admitted. Attached records, pagination and probe replies require
the session accepted for that specific RPC, never a newer session looked up by
identity. Stale owners cannot borrow current authority. At the 4,096-session
limit, known identities may replace their epoch; new identities are rejected
with an observable capacity warning.

The existing keepalive `Ping`/`Pong` has no D5 attempt correlation. It is useful
for exercising the transport path, but is not a membership probe or a failure
detector. Expiry and local queue/send rejection are local events; a peer cannot
manufacture those observations by supplying its own timestamps.

The membership view may exceed the active connection set. Probing a known
member without a current session requires a bounded, normally authenticated
dial or a bounded indirect route. Member knowledge does not imply a permanent
full mesh. Endpoint withdrawal alone does not evict an admitted member.
Admission must preserve configured limits and local readiness with empty
initial discovery.

The owner's advertised endpoint is optional. A wildcard listener (`0.0.0.0`
or `::`) without an explicit advertised endpoint starts normally but publishes
no owner endpoint; it must not advertise the wildcard as a dialable contact.
Existing concrete outbound contacts remain available for control exchanges.
An explicitly configured advertised endpoint uses the resolved listener port
when its configured port is zero.

Disseminate membership changes independently of CRDT operations. Coalesce
superseded updates, limit retransmissions and prioritize self-refutation and
buddy notifications without starving other members. Periodic paginated member
reconciliation repairs missed updates after churn or partition; it uses the
same ordering, admission rules and budgets as incremental updates. It is
separate from D2/D3 data anti-entropy. Retain bounded recovery contacts so that
`Dead` judgments on both sides of a partition cannot permanently prevent rejoin.

Control uses separate bounded one-shot TCP/TLS connections on the existing
listener, not the data writer or data event queue. Priority queues on a shared
stream cannot preempt a frame or blocked event delivery. The measurements below
validate the separate path against those dependencies; CPU, network capacity,
pre-handshake admission and executor scheduling remain shared resources.

##### Transport decision experiment

TCP delivers bytes in order. A control message behind a large data frame cannot
overtake it merely because its queue has higher priority. A separate connection
can remove that particular dependency, but still shares CPU, network capacity
and possibly runtime scheduling with data traffic. Neither paper determines
which adaptation is sufficient for Numax.

Validation uses a declared load envelope and control-delay budget. Sustained
and deployment-scale measurements must complement the local dependency tests:

| Condition | Measurements and required observation |
| --- | --- |
| Idle and nominal data load | Baseline enqueue-to-send delay, reply latency and event-processing delay |
| Maximum permitted data frames and slow reader | Writer blocking and receive-path delay; whether control meets the predeclared budget |
| Saturated data and control queues | Bounded memory/work, explicit admission or enqueue rejection, no remote failure inferred from local rejection |
| CPU-bound WASM and slow storage | Local scheduling delay, health feedback, refuted suspicions and false failure declarations |
| Real peer failure and restored partition | Detection latency, reconciliation and useful recovery contacts |

Record distributions and maxima over the declared run, frame sizes, queue
limits, resource use and violations. Meeting a budget in that envelope is
measurement evidence, not a bound under arbitrary stalls. Choose a shared
stream only with evidence that its blocking is acceptable; a separate path
must also pass the lifecycle, authentication and resource-limit checks.

###### Reader-level causal check

`node::tests::control_progress_isolated_from_shared_writer_and_event_backpressure`
uses the production reader and frame codecs on two plain loopback TCP streams.
The input frames are confirmed queued before the reader is polled. It runs
both JSON and Bincode with an event-channel capacity of one:

| Barrier | Shared path | Independent test path |
| --- | --- | --- |
| Held writer mutex | A `PushOps` event is delivered, then replying to `Ping` waits for the writer | Its separate reader/writer returns `Pong` while the shared writer is still held |
| Full event channel | A second `PushOps` waits for event delivery before the following `Ping` can be processed | Its separate reader/writer returns `Pong` before the shared event channel is drained |

Both reader futures are driven while waiting for the independent reply. The
shared reply remains pending before the explicit release and completes after
it. Shutdown then completes both readers and releases their writer references.
Five-second timeouts bound test liveness only; they are not detector parameters
or latency acceptance budgets.

Run the scoped check and print its four JSON observation records with:

```sh
cargo test -p nx-net control_progress --lib -- --nocapture
```

Each record reports codec, barrier, frame sizes, channel capacity, local
`write_all` durations, independent write-start-to-observed-Pong time, and shared
write-end-to-release/release-to-observed-Pong times in microseconds. These are
test-observer intervals: the independent interval includes reader setup and
scheduling, not just network RTT. Socket write completion is not remote
application acceptance. No tight timing assertion determines success.

The check establishes reader/writer and event-path dependency separation for
small frames. It bypasses TLS, handshake/admission and the `NetNode` connection
registry; it does not implement a second production control connection. It
does not measure maximum-frame or slow-reader blocking, queue byte budgets,
CPU-bound WASM, slow storage, partitions or sustained-load latency. The full
experiment above and a declared control-delay budget remain necessary before
selecting transport isolation. Plain TCP results do not establish secure TLS
performance or control-message authorization.

###### Production-path validation

`control_rpc_meets_local_budget_while_maximum_data_frame_is_blocked` exercises
the actual protocol-9 handshake and RPC path in JSON/Bincode and plain TCP/mTLS.
One data connection exhausts the configured peer limit, its event queue has
capacity one and is full, and a 16 MiB maximum payload remains blocked on its
writer until after the control response. The predeclared local response budget
is 500 ms. Observed responses were approximately 0.5–0.7 ms for TCP and
2.9–3.3 ms for mTLS. The test prints structured observations and asserts that
data remains blocked throughout. This is a local envelope, not a WAN SLA.

RPC tests also reject a certificate/claimed-ID mismatch, wrong request IDs,
inconsistent codecs and incompatible versions. Saturation returns `RateLimited`
without registering a data peer. Three-node runtime tests exercise automatic
probes, helper ACK/NACK, self-refutation, crash detection, durable leave and
restart with a greater epoch. Storage-pause tests obtain probe ACKs before
releasing a terminal flush; fault tests fence without publishing `Left`.
A current-thread test runs a real infinite-loop WASM guest while a probe
completes, with zero failure declarations in that short observation.

```sh
cargo test -p nx-net control_rpc --lib -- --nocapture
cargo test -p nx-core production_adapter --lib -- --nocapture
cargo test -p nx-core sync_manager_owns_automatic_membership --lib
```

Protocol 9 defines the control exchange under
[Wire Versioning](/numax/design/wire-versioning/). Persisted membership metadata
retains its separate version-1 contract under
[Schema Versioning](/numax/design/schema-versioning/#membership-metadata).
Historical fixtures are unchanged. The actual v0.1.5 binary is rejected in both
codecs by the multiprocess compatibility test; no CRDT operations are exchanged.

| Resource | Required bound |
| --- | --- |
| Member knowledge | Members, candidate claims, endpoints, retained floors and serialized bytes |
| Probing | Active attempts, helper fanout, temporary dials, timeouts and retries |
| Dissemination | Queued entries/bytes, updates per frame, retransmissions and work per tick |
| Reconciliation | Page entries/bytes, pending exchanges and cursor lifetime |
| Execution | Control connections, tasks, send/receive work and shutdown duration |

Numeric runtime limits and defaults are specified above. Local saturation must remain observable and must not
be interpreted as proof of remote failure. Metrics use bounded labels; per-peer
detail belongs in bounded introspection. Expose membership, endpoint reconnect
health, active sessions and data-recovery status separately.

##### Required integration contracts

The integration enforces the following contracts. Local tests establish these
boundaries; unmeasured deployment settings are not production guarantees.

| Boundary | Implemented contract | Discriminating check |
| --- | --- | --- |
| Durable identity and replay rejection | Version-1 metadata, checked epoch, serial blocking worker and guarded completion | Interrupted writes and flushes preserve rejection or fail closed; probes progress during pending I/O |
| Control authority | Immediate TLS identity, per-epoch sessions, direct owner revalidation and explicit known-pair leave relay | Identity mismatch and unknown relayed subjects do not acquire authority |
| Probe and local health | Dispatch notification, bounded helper/NACK service and local scheduling-delay cancellation | ACK/NACK correlation, genuine crash detection and no remote evidence from local rejection |
| Suspicion | Health-scaled timing, distinct admitted reporters, bounded coalescing and reconciliation | Repeated confirmations do not reset the start; owner refutation supersedes old timers |
| Deterministic time | Explicit core time/choice inputs; monotonic clock and seeded PRNG in the adapter | Identical core event/choice traces yield identical actions |
| Transport and resources | Independent control path and numeric admission/queue/frame limits | Maximum-frame and saturated-data tests meet the local budget; control saturation is explicit |
| Compatibility | Protocol 9 and unchanged membership schema 1 | JSON/Bincode rejection, actual previous binary and unchanged historical fixtures |

#### Ownership and lifecycle

Place the protocol state machine in dedicated `nx-core` membership modules,
separating transitions, probe scheduling, dissemination and the runtime adapter.
It consumes explicit events/time and emits bounded actions; production and
simulation share these transitions, seeded randomness and stable event order.
The adapter owns I/O and persistence; no storage or network await occurs inside
a transition. `SyncManager` owns startup, cancellation and task completion.
`peer.rs` retains endpoint reconnection responsibility; `nx-net` owns encoding,
framing and transport. Membership does not belong in the CRDT domain `nx-sync`.

An empty discovery set is valid. Local startup must not wait for remote probes
or cluster convergence. Graceful shutdown attempts owner-issued leave within a
finite budget, then cancels and joins owned work even if dissemination fails.
Partial startup failure rolls back its resources. Shutdown remains terminal;
crash recovery advances the durable start epoch before re-admission.
Membership transitions never delete D2 receipt frontiers or establish D3 data
completion.

#### Membership acceptance criteria

| Test boundary | Required evidence |
| --- | --- |
| Ordering | All state/pair combinations, duplicate updates, delayed messages, same-pair precedence and leave closure; timers for superseded pairs have no effect. |
| Persistence | Restart and interrupted epoch/floor persistence preserve monotonicity or fail closed; corruption, overflow and write/flush errors do not publish a new epoch. |
| Probing | Direct success, indirect success, all helpers timing out, no available helper, negative reply followed by timely success, and stale/session-mismatched replies. |
| Lifeguard | Saturating health feedback and recovery, unique corroborators, suspicion deadline bounds, buddy priority and bounded fairness. |
| Admission | Unknown relayed subjects stay candidates; direct owner/session mismatches and unauthorized routes are rejected under the configured identity policy; insecure mode remains unverified. |
| Resources | Every configured limit, churn, queue saturation, slow readers and blocked data processing preserve bounded work and explicit degradation. |
| Recovery | Sparse graphs, mutual failure judgments across a partition, reconciliation, restart, leave/rejoin and expired detailed records do not cause stale resurrection. |
| Lifecycle | Empty startup, stalled handshake, partial-start rollback and shutdown with active probes leave no owned task running. |
| Compatibility | JSON/Bincode frames and semantic validation, historical fixture preservation, exact-version rejection including the real v0.1.5 binary. |
| Data integration | Membership churn leaves durable CRDT/receipt state intact across all six families; `Alive` never certifies data convergence. |

Start with deterministic transition tests, then use real TCP/TLS and
multiprocess integration under load. Record seeds, topology, membership size,
limits, timing, scheduling delays, probe/ACK latency, refuted suspicions,
false failure declarations, true-failure detection latency and resource use.
Transport packet loss and simulated message loss require separate results.
The one-hour nominal run and the combined 50-node partition/recovery criteria
remain the release obligations in the [roadmap](/numax/roadmap/); unit tests and
this proposed contract do not establish those results.

### D7 — Gossip and repair scheduling

The runtime now sends local operations to a bounded K-fanout and forwards newly
accepted remote `Push` and `Replay` entries only after persistence and in-memory
publication. The original sequenced entry is preserved; duplicates and
`TrackingLimit` rejections are not forwarded. No wire message or schema change
was required.

The current local estimate is `N = 1 + connected data peers`; it is not a
cluster-wide membership count. The requested fanout is
`min(ceil(log2(N) + c), gossip_max_fanout, eligible peers)`. Empty and small
views naturally clamp to available peers. The implementation sorts active
connection addresses and advances a shared cursor to rotate the selected peers;
remote forwarding excludes the immediate source. `SyncConfig` and the CLI expose
`gossip_fanout_constant` (default 1) and `gossip_max_fanout` (default 64) through
`SyncConfig` and the CLI's `[limits]` TOML section, not dedicated flags.
These defaults are provisional: `c` has not been empirically calibrated, and
the current neighbor selection is not topology-aware.

The remote forwarding queue is bounded by `queued_ops_limit`; a forward batch
is bounded by the existing operation limit and by the smaller of
`max_message_size` and the replay byte budget, with room reserved for framing.
A full queue or failed send drops only that forwarding attempt. The persisted
per-origin history and existing paginated, byte-bounded anti-entropy remain the
repair path within D3 retention. A three-node TCP test verifies multi-hop
forwarding with both JSON and Bincode, and apply tests cover all six CRDT
families plus duplicate suppression.

Adaptive pacing based on load/RTT, jitter, calibrated retry budgets, topology-
aware contacts and the 50-node loss/partition acceptance run are not yet
implemented or validated. In particular, a send completion duration is not
being treated as peer RTT. Do not infer convergence beyond the bounded D3
history/retention contract from a successful gossip send.

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

The following aspects remain open for the proposed capabilities:

- A failure model covering OS restart, power interruption or storage-device loss.
- Recovery beyond the bounded D3 baseline and retention contract.
- Membership control-message authorization/encoding, future floor compaction,
  runtime probe/suspicion parameters and transport byte budgets.
- Shared-stream versus separate-connection control isolation, supported by measurements.
- Empirical fanout calibration, topology-aware neighbor selection, jitter and
   adaptive pacing based on load/RTT.
- Manual candidate removal and API error semantics.
- Membership wire compatibility, future metadata migrations and expanded state-transfer formats.

The RFC does not assign defaults or guarantees to these unspecified parameters.

## Failure scenarios that define the contract

| Scenario | Required observation or unresolved choice |
| --- | --- |
| Crash between local state and replay persistence | D1 writes both in one atomic batch for all six families; recovery must not expose a surviving new state without its operation identity and replay metadata. The batch may survive before flush without having been accepted. |
| Crash after a batch but before flush or response | The documented D1 durability and retry semantics must match what survives a restart; unknown outcomes cannot be silently turned into acknowledged success. |
| Remote op persisted but forwarding fails | The accepted op remains recoverable by the repair path; a send failure consumes bounded work and does not silently lose the sole source. |
| Sole durable holder fails permanently | Permanent loss of the sole durable copy is outside the D1/D3 guarantee. Local acceptance does not require a remote durability acknowledgement. |
| A newer op arrives before an older one | The receiver must find and request a genuine hole or reconcile safely. Neither `last OpId` nor timestamp order alone establishes completeness. |
| Duplicates arrive after seen-ID eviction | Compacted receipt prefixes suppress covered positions independently of seen-ID and replay retention. Missing history requires replay or a validated baseline. |
| Partition outlasts retention | Joining partitions report an unrecoverable gap or use an explicitly selected state-transfer path. No false convergence signal. |
| New member joins after useful history expires | Admission and membership visibility are separate from data readiness; the node cannot claim caught-up state without a validated bootstrap/recovery path. |
| Peer connects before finishing repair | Connection and membership may be healthy while data is still incomplete. Report pending or failed recovery until the D10 completion proof succeeds. |
| Healthy member is CPU- or storage-stalled | Suspect and refutation are observed separately from a final failure declaration; control work remains schedulable under load. |
| Node restarts, then an old packet arrives | Durable per-origin coverage suppresses old sequenced replay. The proposed membership protocol separately orders incarnations and rejects stale identity updates. |
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

### Verification boundaries

| Contract | Verification boundary |
| --- | --- |
| D1 | Atomic persistence, flush-confirmed success, uncertain outcomes and process restart |
| D2 | Out-of-order delivery, compacted duplicate suppression, bounded gap tracking and replay |
| D3 | Complete-cut baseline installation, expiry, legacy state and explicit recovery refusal |
| D4–D6 runtime membership | Shared core, durable recovery, owned lifecycle, authenticated control, real-node probes/refutation/leave/restart, maximum-frame isolation and short CPU/storage-load tests; release-scale detector criteria remain open |
| Proposed D7 | Sparse topologies, bounded fanout, forwarding and anti-entropy |
| Proposed D8–D10 | Management API, compatibility, readiness and convergence evidence |

Component tests exercise the owning state machine; integration tests cover
storage/network boundaries; end-to-end cases cover each CRDT family. The
[roadmap](/numax/roadmap/) defines the release-scale criteria. Unit and local TCP
coverage do not establish the 50-node loss/partition or one-hour detector targets.
