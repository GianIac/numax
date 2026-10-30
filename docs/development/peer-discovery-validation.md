# Peer discovery validation notes

   Reference for the v0.1.6 discovery, membership and replication work.
   These notes collect verification procedures and previously recorded local
   observations from the former public RFC and personale note. 
   Recorded runs are historical evidence, not results of a new run or proof 
   that the release criteria have been completed.
   Use the current source and CI workflows when reproducing them.

   The user-facing contracts are in the site pages for Peer Discovery, the
   Discovery Provider Contract, Schema Versioning and Wire Versioning. Release
   acceptance criteria and completion status remain in the roadmap.

#### Acceptance tests

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

#### Wire/schema evolution and historical-data verification

Wire and storage versions are independent. Protocol 8 requires exact peer
version equality; op-log and sequence schema 3 reject writers that cannot
preserve baseline and receipt metadata. The compatibility matrix and offline
upgrade procedure are specified in
[Schema Versioning](https://gianiac.github.io/numax/design/schema-versioning/#upgrade-historical-state-and-downgrade-contract).
[Wire Versioning](https://gianiac.github.io/numax/design/wire-versioning/#compatibility-verification-and-deployment)
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
[compatibility verification](https://gianiac.github.io/numax/design/schema-versioning/#compatibility-verification).

## Scheduling observations

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
[Wire Versioning](https://gianiac.github.io/numax/design/wire-versioning/). Persisted membership metadata
retains its separate version-1 contract under
[Schema Versioning](https://gianiac.github.io/numax/design/schema-versioning/#membership-metadata).
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

## Topology verification

Deterministic component tests cover bounded rotating recovery windows, fair
first turns, `Dead`, `Left`, withdrawal/re-addition and invalid/local endpoints.
Real loopback TCP tests cover a full one-slot relay recovering all six CRDT
families with JSON and Bincode, and two full pairs reconnecting after a gated
data partition with mutual `Dead` inputs. The tests feed explicit membership
fixtures into the production adapter; they do not measure detector accuracy or
claim packet-loss results. Storage assertions check materialized values and complete durable state,
including ORSet removal tags and RGA tombstones. A further TCP test admits an
owner through real membership control and obtains its data endpoint without
discovery or a fabricated membership snapshot. A transport regression verifies identity-guarded slot release even when
the disconnect-event queue is full. The 50-node and one-hour release criteria
remain separate obligations.

Reproduce these focused checks with:

```sh
cargo test -p nx-core sync_manager::neighbors
cargo test -p nx-core topology::
cargo test -p nx-net topology_disconnect
```

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
remain the release obligations in the [roadmap](https://gianiac.github.io/numax/roadmap/); unit tests and
this proposed contract do not establish those results.

#### Runtime-management verification

`cargo test -p nx-core -p nx-api manual -- --nocapture` covers authenticated HTTP
acceptance, validation, duplicate/capacity errors, cursor isolation, disabled and
terminal lifecycle, concurrent registration, source coexistence, startup rollback,
storage write/flush failures, and actual process termination before batch,
before flush and after flush. Recovery introspection exposes a known gap and a
source-specific refusal without creating an active connection.
Regressions also hold a flush across the shutdown deadline, distinguish queued
from started requests, retry the same manager after bind failure, roll back a
failed activation without retaining services, suppress a persisted local endpoint
before the global capacity check, and retain the recovery-scan permit after
request cancellation. `cargo test -p nx-core rollback --locked` exercises all
integrated dynamic-provider rollback hooks; mDNS uses its injected fake daemon
and DNS uses an injected resolver, not external network services.

Local TCP tests exercise normal admission, rejected self-identity handshakes,
membership probes and reconnection. Each of the six CRDT families recovers
operations written before candidate insertion and then replicates new mutations,
including removal semantics, using both JSON and Bincode and checking materialized
and durable state. `cargo test -p nx-core delivery::tests` covers queue accounting;
`cargo test -p nx-core peer_metrics` checks that metric-series identities stay fixed.
The workspace suite and Rust fmt/check/Clippy controls also pass.

These are local transport and process-crash checks, not power-loss guarantees,
new TLS certification, previous-binary compatibility evidence, packet-loss
experiments or release-scale convergence/detector validation.

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
[wire versioning procedure](https://gianiac.github.io/numax/design/wire-versioning/): test JSON and
Bincode bytes and semantics, update rejection tests, and run a v0.1.5 binary
against the new binary. A mismatch must be rejected before peer registration or
CRDT exchange, without panic or datastore mutation.
A storage change needs an explicit migration and historic fixture checks under [schema versioning](https://gianiac.github.io/numax/design/schema-versioning/); never rewrite fixtures to match new behavior.

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
| D8–D10 runtime management | Persistent candidate POST, compatible default GET, independent observation views and bounded metrics; local acceptance/probe/reconnect/recovery evidence above. Previous-binary compatibility and release-scale convergence remain separate checks |

Component tests exercise the owning state machine; integration tests cover
storage/network boundaries; end-to-end cases cover each CRDT family. The
[roadmap](https://gianiac.github.io/numax/roadmap/) defines the release-scale criteria. Unit and local TCP
coverage do not establish the 50-node loss/partition or one-hour detector targets.


## Discovery provider verification

Deterministic unit and component tests cover static compatibility, bounded
watch overflow, snapshot revision continuity, late candidate arrival,
overlapping source contributions, source removal, startup rollback and
cancellation-safe shutdown. Provider-specific tests additionally cover:

- bootstrap TTL expiry during a stalled seed query, bounded responses,
  authenticated TLS/allowlist rejection, seed loss, restart and withdrawal;
- DNS-SRV ordering, filtering, refresh, validity expiry, transient failure,
  recovery and cancellation of an in-flight lookup;
- file creation and removal, atomic replacement, malformed and non-UTF-8
  updates, last-good retention, recovery and shutdown;
- mDNS address and instance bounds, self filtering, removal and service-name
  conflicts, shutdown during replacement, missing withdrawal acknowledgements,
  preservation of both owned keys and non-renewable cleanup deadlines.

Regression coverage also exercises observation freshness versus cached replay,
resubscription timestamps, global mDNS retained-state bounds, bounded shutdown
acknowledgements, non-blocking startup dialing and anti-entropy over active
connections independently of discovery churn. Capacity tests cover the accepted
maximum, rejection of zero, maximum-plus-one and `usize::MAX`, legacy static
normalization with ordered duplicate peers, and defensive internal channel
rotation. Lifecycle tests cover delayed cleanup as a restart barrier, worker
and cleanup panics, concurrent resubscription, and terminal explicit shutdown.
Test presence is not evidence
that every environment-dependent scenario has run successfully.

The ignored
`discovery::mdns::tests::two_daemons_discover_and_remove_an_announced_endpoint`
test exercises two real DNS-SD daemons over local multicast, including goodbye
removal. CI runs this check explicitly on a dedicated macOS runner; keeping it ignored prevents the ordinary cross-platform suite from failing on hosts or containers without multicast support. Run it manually on a multicast-capable host with:

```sh
cargo test -p nx-core \
  discovery::mdns::tests::two_daemons_discover_and_remove_an_announced_endpoint \
  -- --ignored --exact
```

CI also explicitly selects
`discovery_lan::mdns_three_daemons_recover_missed_crdt_ops_after_restart` from
the CLI multiprocess suite on macOS, with `NUMAX_MDNS_E2E=1` and
`NUMAX_MDNS_LAN_IP` derived from a real local interface. It builds both reader
and writer variants of the `discovery_lan` guest first. The generic Ubuntu
ignored-test invocation excludes this multicast-specific module.

That E2E uses three real daemon **processes on one host**, without `--peer`,
and checks discovery, CRDT replication, missed-operation recovery after restart
within a configured 128-operation retention bound, stable identities and
shutdown. It is not evidence of a run on three separate LAN devices or of
recovery beyond retained history. The three-device LAN demo remains a separate
release closing check; neither provider-test presence nor CI wiring asserts it
has passed.


<span id="regression-evidence"></span>

## Storage compatibility verification

Historical fixtures are immutable logical-store records produced by their
release encoders. They include source tags, pinned commits and reproduction
instructions; current encoders must not regenerate them to fit a migration.

| Fixture | Contents |
| --- | --- |
| `sync_manager/fixtures/v0_1_0/` | Unversioned sync records and operation payloads |
| `sync_manager/fixtures/v0_1_5/` | Six CRDT families, tombstones, seen IDs, UUID-only replay, schema-1 headers and an application key |

Verification reconstructs disposable stores and checks the following boundaries:

- Payload bytes and unrelated application records survive migration unchanged.
- Record checkpoints resume after reopen, including partially converted receipts.
- Storage rejection or corrupted input does not advance the failed record's checkpoint.
- Final schema headers and checkpoint removal remain atomic across process termination.
- Legacy replay remains rejected after real seen-ID and op-log eviction.
- A new sequence frontier does not establish receipt of a historical baseline.
- Baseline installation preserves full internal state, receipt coverage and legacy
  provenance across restart, delayed duplicates and flush failure.
- Recovery refusal remains specific to the requested source and current coverage;
  an alternate source can still complete recovery.

Focused verification entry points:

```sh
cargo test -p nx-core baseline_
cargo test -p nx-core v015
cargo test -p nx-core sequence_v1_migration
cargo test -p nx-core migrates_fixture_generated_by_v0_1_0
cargo test -p nx-net
```

For real-process compatibility, build `examples/distributed_counter` for
`wasm32-unknown-unknown`, build `nx-cli` from the pinned v0.1.5 sources, set
`NUMAX_PREVIOUS_NX_BIN` to that release binary, then run:

```sh
cargo test -p nx-cli --test multiprocess_smoke \
  different_protocol_versions_reject_connection_without_exchanging_ops \
  -- --exact --ignored
```

These tests require local TCP sockets; the process-crash test also requires
permission to spawn and terminate its child process. They never operate on an
operator's datastore.

