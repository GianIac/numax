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

### 50-node simulated partition: local observation

On 2026-11-05, a macOS run at commit
`f0797370534722c2b2f6524692948bc85d8ee6e6` passed the following test with
seed 42 and the unmodified production `SyncConfig` defaults:

```sh
cargo test --release --locked -p nx-core \
  sync_manager::manager::tests::determinism::split_brain_50_nodes_production_defaults_json \
  -- --ignored --exact --nocapture
```

The test reported `1 passed; 0 failed`. The initial chain formed after 134
virtual seconds; the bidirectional partition then lasted 60 virtual seconds.
After rejoin, all data converged in 5 virtual seconds and membership recovered
in 8. The 600 operations covered all six CRDT families, with no writes after
rejoin. The test took 223.49 seconds of wall time; one membership flush took
25.052 seconds and was followed by queue waits. The cause was not established.

The run used simulated message loss, not packet loss over real TCP. It is
one observation for the declared seed and workload, not evidence for the
separate continuous-write, packet-loss, retention, rolling-restart or one-hour
detector scenarios. `complete_origins_per_node_at_limit` is null because the
test completed before the 30-second sample.

```json
{"anti_entropy_interval_ms":30000,"codec":"Json","complete_origins_per_node_at_limit":null,"data_converged_after_s":5,"groups":[[0,24],[25,49]],"incomplete_at_limit":[],"latency_ms":5,"loss":"10% independent seeded draws per directed (from, to, purpose) stream, for the whole run including partition and recovery","max_peers":64,"membership_formed_s":134,"membership_healed_after_s":8,"network":{"lost_partition":23286,"lost_random":14182,"queue_full":0,"rejected_delivery":0,"sent":127581},"nodes":50,"observation_limit_s":180,"operations_total":600,"partition_duration_s":60,"partition_start_s":134,"payload":"keys det-*, values of at most 7 bytes","reconnect_backoff_ms":[500,30000],"recovery_errors_total":0,"recovery_limit_s":30,"recovery_timeline_min_coverage_cross_connections":[[42,1250],[42,1250],[47,1250],[47,1250],[50,1250],[50,1250],[50,1250],[50,1250]],"retention_operations":10000,"sampling_s":1,"seed":42,"service_ms":1,"socket_timeout_ms":30000,"timing":"production-defaults","topology":"initial chain candidates (index +/- 1), production neighbor selection thereafter","version":1,"wall_ms":221396,"writes":"12 operations per nodeover six CRDT families at partition start; no writes after rejoin"}
```

### Split-brain recovery and expired retention: local verification

On 2026-11-05, the existing TCP partition tests passed with JSON and Bincode
at base commit `d4f9edbe5dc7a2d6a9557ec78fcd3247220e33f9`. They exercise
independent writes on all six CRDT families, mutual `Dead`, rejoin and complete
recovery within 30 wall-clock seconds, comparing durable, materialized and
in-memory state and contiguous coverage.

A new uncommitted TCP regression,
`split_brain_expired_history_fails_explicitly_over_real_tcp_json_and_bincode`,
also passed with both codecs. Two isolated nodes each accept 12 operations
across the six families with retention limited to one operation. Reopening their
stores discards pending broadcast queues while preserving durable state, so
repair cannot accidentally use an unretained operation still queued in memory.
The nodes then connect through real loopback TCP.

After both summaries are known, neither source has a complete baseline:
each knows 12 missing operations from the other origin. Both report
`SourceIncomplete`, retain remote contiguous coverage of zero with a known
high sequence of 12 and a missing range of 1 through 12, and preserve local
contiguous coverage of 12. The test compares all 12 durable/materialized state
tables byte-for-byte against their isolated snapshots and checks all six
in-memory values. It does not interpret connection or equal visible values as
convergence.

Reproduce the focused checks with:

```sh
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::topology::real_control_data_partition_ \
  -- --nocapture --test-threads=1
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::recovery:: \
  -- --nocapture --test-threads=1
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::sequencing::sequenced_expired_history_reports_source_unavailable_without_false_completion \
  -- --exact --nocapture
```

Recorded results: two TCP partition tests passed in 29.35 seconds; all 18
recovery tests passed in 11.20 seconds; the separate expired-history test
passed in 0.49 seconds before the new regression was added. The recovery suite
also verifies successful six-family baseline repair after history eviction,
explicit coverage-conflict refusal, preservation across restart and repair
from an alternate qualifying source.

These are local component and loopback TCP observations, not the 50-node
campaign or a real packet-loss experiment. The retention regression begins
with isolated writes and reopens stores before connecting; it does not
simulate an established cluster's timed network partition. Explicit refusal
is source-specific, not proof that no future source can repair the gap.

The first parallel workspace run timed out in the existing
`baseline_unrecoverable_source_is_observable_and_another_source_can_repair`
test, which had passed in the focused run. The new regression now uses the
existing bounded child-process helper to avoid adding sled process-global
storage contention to unrelated tests. This does not establish the timeout's
root cause. With that isolation, `cargo fmt --all -- --check`,
`cargo check --workspace --all-targets`,
`cargo clippy --workspace --all-targets -- -D warnings` and
`cargo test --workspace --verbose` all passed. The final nx-core unit run
reported 587 passed, zero failed and seven ignored. Ignored release-scale
and environment-specific tests were not run by the workspace command.

### Sparse topology, delayed duplicates and persistence faults

On 2026-11-05, at base commit
`d4f9edbe5dc7a2d6a9557ec78fcd3247220e33f9` with local test changes, a focused
release-profile campaign exercised the following matrix:

| Boundary | Evidence and assertions |
| --- | --- |
| Sparse TCP topology | `rotating_full_slot_recovers_six_families_after_data_partition` runs three nodes with at most one data peer each and a rotating relay, with JSON and Bincode. All six families converge in memory and durable/materialized state. The strengthened condition also requires contiguous coverage of all 11 operations from each of the two writers at every node, no sparse positions, no missing/unconfirmed baseline and no recovery failure. |
| Delayed duplicate replay | `sequenced_all_families_ignore_delayed_replay_after_eviction_and_restart` delivers dependencies in reverse order, evicts seen/replay entries with limits of one, replays duplicates and reopens the receiver. The strengthened post-restart assertions compare all six durable states against the source, all materialized/in-memory values and complete contiguous coverage. ORSet removals and RGA tombstones remain preserved. |
| Storage errors | Per-family local tests inject batch and flush failures, verify publication/retry boundaries and reopen consistency. `remote_apply_storage_failure_returns_no_forwardable_operations` rejects a six-family batch with no durable, materialized, in-memory or receipt changes, then verifies complete retry recovery and suppression of duplicate forwarding. Baseline tests additionally cover failed installation and unconfirmed flush fencing. |
| Interrupted persistence | `local_crdt_process_crash_preserves_complete_batches` terminates child processes for ten mutation cases across all six families, before batch, before flush and after flush (30 cases). Reopen verifies state, materialized values, seen/replay records and receipt/position consistency. The sequenced remote-crash test covers GCounter; the baseline process-crash test covers a complete six-family cut. Cancellation tests separately verify completion by owned storage workers. |

The initial focused campaign passed 94 tests: 11 topology tests in 36.93
seconds, 27 sequencing tests in 47.28 seconds, 18 recovery tests in 11.35
seconds, 37 local-path tests in 20.51 seconds and the remote disk-full
regression in 0.08 seconds. After strengthening the three regressions above,
they passed individually in 4.37, 0.95 and 0.23 seconds respectively.

After these changes, formatting, workspace check and Clippy all passed.
The parallel `cargo test --workspace --verbose` run did not pass: nx-core
reported 586 passed, one failed and seven ignored. The failing pre-existing
test was
`baseline_unrecoverable_source_is_observable_and_another_source_can_repair`,
which timed out waiting for alternate-source repair. The target still had
contiguous coverage zero, known high sequence 12 and a visible
`SourceIncomplete` refusal from the incomplete source; both sources remained
connected. The strengthened regressions passed in that workspace run.

Running the failing test alone in the exact workspace test executable passed
in 0.76 seconds. This is consistent with execution-load sensitivity but does
not prove its cause or invalidate the timeout. At that stage, the owner chose to preserve
that test unchanged, without additional process isolation or larger timeouts.
The roadmap item remained unchecked pending resolution of the full-suite
failure; the focused successes are evidence, not a fully green validation.

Reproduce the matrix with:

```sh
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::topology:: -- --nocapture --test-threads=1
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::sequencing:: -- --nocapture --test-threads=1
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::recovery:: -- --nocapture --test-threads=1
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::local_ -- --nocapture --test-threads=1
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::disk_full_does_not_expose_partial_remote_state \
  -- --exact --nocapture
```

These are complementary tests, not one experiment injecting every fault into
the sparse network simultaneously. The rotating-relay test feeds controlled
membership inputs into production reconnect logic; it is not evidence of
detector accuracy. Real TCP is loopback, without packet-loss injection.
Disk-full errors are injected; child termination exercises process crashes,
not power failure or torn-device writes. No universal convergence or
durability guarantee beyond the documented recovery model is inferred.

### Three-node rolling restart and full-cluster shutdown

On 2026-11-05, at base commit
`d4f9edbe5dc7a2d6a9557ec78fcd3247220e33f9` with local test changes, both
`restart::rolling_restart_every_node_recovers_six_families_json_and_bincode`
and `restart::full_cluster_shutdown_recovers_six_families_json_and_bincode`
passed. The final focused run reported two passed, zero failed in 48.68
seconds. Each test exercises JSON and Bincode.

The fixture uses three nodes, real loopback TCP, static full-mesh candidates,
at most two data peers per node, a 25 ms anti-entropy interval, 10–40 ms
reconnect backoff and the default 10,000-operation retention. Each node has
an independent temporary on-disk datastore and a stable test NodeId.

| Scenario | Procedure and verified outcome |
| --- | --- |
| 100% rolling restart of this fixture | Start with 12 accepted operations per node across all six families and await convergence. Stop and drop one manager, verify the datastore file lock is released, write 12 more operations on each survivor, reopen the stopped node's store and await full recovery. Submit 12 new operations on the restarted node and await convergence before stopping the next node. Repeat once for each of the three nodes. Final contiguous coverage is 48 operations per origin, 144 total. |
| Full-cluster shutdown, tested separately | After initial convergence, request all three shutdowns concurrently and drop every manager. Verify that every datastore is released before starting any replacement. Reopen all three stores, await recovery before new writes, then accept 12 more operations per node and converge again. Final contiguous coverage is 24 operations per origin, 72 total. |

Both scenarios compare each node's six durable CRDT states immediately after
reopening, before its networking starts, against the snapshot taken at shutdown.
They require stable NodeIds, newer replication generations and membership
epochs, and new local operations stamped with the current generation and
monotonic sequence. Recovery requires every current membership epoch to be
`Alive`, active data connectivity, complete contiguous coverage of every
origin, no sparse positions, no missing/unconfirmed baseline and no recovery
failure. All six in-memory values, materialized values and durable states
are checked, including ORSet tags and RGA tombstones. Each recovery phase
has a 10-second wall-clock limit, with an explicit elapsed-time assertion.

```sh
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::restart:: -- --nocapture --test-threads=1
```

Each test runs in a bounded child process to isolate unrelated sled workloads.
Within that process, a node restart replaces a SyncManager and reopens its
store; it is not a restart of a separate CLI daemon process. These are
orderly shutdowns, not abrupt process death, power loss or a rolling restart
of 50 nodes. At that stage, the earlier sparse-topology/full-suite timeout
remained a separate open item.

Formatting, workspace check and Clippy passed after the final test changes.
The full workspace test command failed in the previously open
`baseline_unrecoverable_source_is_observable_and_another_source_can_repair`
test, this time while waiting for the source-scoped refusal. Both restart
tests passed in the same run; nx-core reported 588 passed, one failed and
seven ignored in 224.49 seconds. The existing failing test was left
unchanged as requested. The rolling-restart roadmap checkbox remained open:
the local three-node evidence is recorded, but full validation is not green
and no separate-daemon or 50-node campaign was executed.

### Resolution of alternate-source recovery test interference

At the same base commit, subsequent diagnosis reproduced the alternate-source
timeout with only the recovery, sequencing and local-path groups running on
12 test threads: 81 tests passed and the alternate-source test failed.
Independent execution of the full nx-core binary passed, confirming that
one green rerun alone was insufficient evidence of a resolution.

Inspection of sled 0.34.7 confirmed that its concurrency-control lock is global
to the process, not to an individual datastore. Stack sampling under concurrent
load observed the recovery test blocked in synchronous `sled::flush` while
constructing its target, and an unrelated summary-budget test waiting for the
global batch write lock. The construction runs on the test's current-thread
Tokio runtime. Thus independent test databases can interfere with both storage
progress and async scheduling; distinct directories do not isolate this load.
These observations establish a test-load isolation defect, not a universal
diagnosis of all possible recovery delays or a production latency guarantee.

With the owner's approval, the alternate-source test now uses the existing
bounded child-process helper, like the lost-push and restart scenarios.
All three managers remain together in the child, using the same real TCP
scenario, current-thread runtime, workloads and ten-second phase timeouts.
Both phases additionally check monotonic elapsed time, so synchronous work
delaying a Tokio timer cannot turn an over-limit completion into success.
The existing 60-second child lifetime bounds setup, both phases and shutdown;
it does not replace either recovery deadline. No production code, timeout
increase, reduced workload, global serialization or skipped test was needed.

Post-correction verification:

| Check | Result |
| --- | --- |
| Exact alternate-source test | 1 passed in 1.65 seconds |
| Combined scenario matrix, exact workspace nx-core executable, 12 test threads | 96 passed, zero failed in 104.21 seconds: topology 11, sequencing 27, recovery 18, local paths 37, remote disk-full 1 and restart 2. This includes the previously failing concurrent group and both codecs in the restart scenarios. |
| `cargo fmt --all -- --check` | Passed |
| `cargo check --workspace --all-targets` | Passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed |
| First `cargo test --workspace --verbose` | Passed; nx-core 589 passed, zero failed, seven ignored in 219.00 seconds |
| Second `cargo test --workspace --verbose` | Passed; nx-core 589 passed, zero failed, seven ignored in 221.85 seconds |

Both full workspace runs also passed the alternate-source and two restart
tests. The sparse-topology and rolling-restart roadmap items are now checked
for the documented complementary tests and three-node real-loopback fixtures.
Restart covers all nodes in that fixture, not 50 separate daemon processes.
This closes the earlier full-suite validation blocker without claiming that
storage contention is harmless in production.

The default workspace runs do not execute ignored tests. No new 50-node run,
real packet-loss injection, one-hour detector campaign, continuous-write
recovery scenario, optional profiling feature run, excluded guest/example
build or ignored multiprocess/compatibility campaign was performed here.

### Existing component and TCP checks

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
