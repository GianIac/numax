# Sync manager tests

The sync manager test suite follows the same ownership boundaries as the
production module:

- Unit and component tests stay beside the production code they exercise.
- Manager integration tests live in `mod.rs`.
- End-to-end convergence tests live in `e2e/`, with one file per CRDT family.
- Shared setup, operation, persistence, and convergence helpers live in
  `support.rs`.
- Deterministic multi-family replay lives in `determinism.rs`; it drives the
  same production loops and membership actor through the internal test ports.
- Rolling and full-cluster restart integration lives in `restart.rs`; it
  reopens on-disk stores and checks all six families over real loopback TCP.

The E2E files must exercise behavior through the sync manager and its handle.
They must not contain family-independent setup or duplicate helpers from
`support.rs`.

When adding a CRDT family:

1. Add its shared test operations and convergence helpers to `support.rs`.
2. Add an `e2e/<family>.rs` file.
3. Register the file in `e2e/mod.rs`.
4. Cover replication, convergence, materialized state, and durable state where
   applicable.

Run the complete suite with:

```bash
cargo test -p nx-core
```

Run the three-node rolling and full-cluster restart checks with both codecs:

```bash
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::restart:: -- --nocapture --test-threads=1
```

Each restart test uses the existing bounded child-process helper. A restarted
node is a new `SyncManager` using a reopened datastore, not a separate daemon
process. Rolling restart covers all three nodes, with recovery between stops;
full shutdown stops every node concurrently before reopening any store. Both
are orderly-shutdown checks, not abrupt crashes or a 50-node campaign.

## Detector rates and one-hour nominal load

`detector_soak.rs` and `detector_process.rs` exercise the production membership
actor and sync loops over real loopback TCP on macOS and Linux. The ordinary suite runs a
three-node, eight-second smoke scenario with JSON and Bincode, including a
real transport outage, plus a three-worker real process crash and a separate
three-process storage-only baseline. The 100-node
scenarios and internal worker entry point are explicitly ignored by default
because of their resource use and duration.

Run the short 100-node preflight before committing to the full hour:

```bash
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::detector_soak::detector_100_nodes_preflight_json \
  -- --ignored --exact --nocapture
```

Run the actual one-hour campaign with:

```bash
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::detector_soak::detector_100_nodes_one_hour_json \
  -- --ignored --exact --nocapture
```

The coordinator runs in an isolated child process. The 100-node scenarios
launch 100 real worker processes, each owning one manager and datastore, not
100 managers sharing sled's process-global lock. These are test workers, not
CLI daemons or WASM guests. Initial candidates form a ring. Before measurement,
all nodes present their real runtime owner records through bounded TCP control
Exchange requests, with at most one setup request in flight per worker.
Explicit rate-limited replies use a bounded retry budget without raising any
transport capacity. This prepares full
membership through normal admission instead of benchmarking organic cluster
formation; it does not fabricate membership states. Production neighbor
rotation follows, with at most four data peers per node.
Detector timings, anti-entropy and retention use
production defaults. Probe choices use production OS randomness; this is a
wall-clock experiment, not seeded deterministic replay or packet-loss testing.

The nominal window starts only after full, healthy membership formation.
The approved 100-node nominal profile offers one GCounter increment every ten
seconds per node on the fixed `detector-nominal` key: ten requests per second
cluster-wide. Node offsets spread requests over the entire ten-second period.
Each worker permits at most four pending submissions, records offered and
flush-confirmed operations separately, and fails on saturation, storage
errors or a missed generation period. The full campaign requires 360 offered
and confirmed operations per node, 36,000 total, after at least one real hour.
The final submissions may drain for at most 40 seconds (four pending
submissions times the ten-second period); detector monitoring
continues during that drain. Confirmations inside the nominal window and
their actual rate are reported separately, not assumed equal to offered
load. Queue pressure and rejections are reported.
The test does not certify replication convergence of those operations.

The earlier 100-request/s profile saturated both the single-process and
multiprocess fixtures on the Mac. Its failures are evidence, not successful
hour campaigns. Reproduce that separate, explicitly demanding profile with
`detector_100_nodes_high_load_preflight_json`, using the same exact-test
command as preflight. Its limits are not relaxed to turn it green.

For a diagnostic comparison without membership, transport or replication:

```bash
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::detector_soak::storage_100_processes_preflight \
  -- --ignored --exact --nocapture
```

This opens 100 independent real datastores in 100 worker processes. It reuses
the same staggered generator, ten requests/s total, 30-second window,
four-pending limit and forty-second final drain as the detector preflight.
Each request atomically increments one eight-byte counter under the normal
storage write lease and waits for a real durable flush. Workers verify the
counter after the load and after reopening the store. A regression test
pauses a flush to verify that confirmation cannot precede it, and covers
flush failures and malformed stored values.

`STORAGE_BASELINE_CONFIG` and `STORAGE_BASELINE_REPORT` explicitly identify
this different workload. Detector counts are null, not invented zero
observations. The baseline does not perform CRDT metadata writes, replicated
writes, network traffic or true-failure detection, and cannot satisfy the
one-hour detector criterion. Compare runs sequentially on an otherwise
comparable host; do not run the baseline alongside the cluster or workspace
suite. The final local baseline confirmed all 300 requests within the
nominal window, with a maximum submit latency of 149.82 ms. This rules out
100 open stores alone as a sufficient explanation for that cluster run's
slow confirmations; it does not rule out storage contention under the much
larger replicated workload.

Nominal suspects are false-positive suspicion observations because no
subject is intentionally failed in that window. Refuted observations are
reported separately. Locally declared Dead and learned Dead are distinct;
either during nominal observation fails the test. Counts are observations
at individual nodes, not deduplicated cluster-wide incidents. Rates use
observer-hours (number of nodes times observed wall-clock hours).
Formation counts are excluded from nominal rates.

After nominal load, new writes stop. The multiprocess test kills and reaps one
actual worker without graceful leave or injected membership verdicts.
The in-process smoke instead drops its membership handle, waits for snapshot
closure, then stops the TCP transport. Survivors must observe Dead within a
120-second observation budget. Reports distinguish each observer's local
declarations from learned verdicts and record sampled suspicion/Dead latency
every 100 ms. Missing suspicion samples can mean a node learned Dead directly.
Latency is measured from the fault request until coordinator receipt of the
sampled state, including IPC/collection delay. The interval until confirmed
process exit or transport shutdown is also reported. Neither case is a
one-way packet filter or packet-loss experiment.

`DETECTOR_CONFIG`, `DETECTOR_NOMINAL_REPORT`, `DETECTOR_FAILURE_REPORT` and
`DETECTOR_REPORT` describe the experiment. CPU time and RSS come from `ps`
for the sum of worker process samples (coordinator excluded) in the 100-node
campaign; 100% CPU means one core. Summed RSS can count shared pages multiple
times and is not unique physical memory. The in-process smoke measures its
whole cluster process. RSS peaks and aggregate queue peaks are sampled, not exact continuous maxima. Resource
sampling errors fail explicitly. The bounded child helper captures output,
so progress becomes visible to the parent after child completion.
Periodic worker status uses one latest-value watch per node; phase changes
and errors use a separate bounded 512-event queue. Slow resource sampling
cannot fill that queue with repetitive status messages. A regression test
verifies coalescing, critical-event delivery and explicit overflow reporting.

`detector_diagnostics.rs` adds test-only aggregate timing of the existing
local-write tracing events: registry-lock wait, blocking-job dispatch,
storage-lease wait, batch execution and durable flush. The isolated network
workers install an event-only subscriber; it records counts, accumulated and
maximum microseconds, and failed batch/flush events, without logging payloads
or individual operation IDs. Nominal and partial reports include these
aggregates under `diagnostics`, alongside existing replication-work counters
and completed explicit store flushes since nominal start. Reading those
counters does not scan the datastore. An in-flight setup operation completing
after nominal start can contribute to the replication counters.

Stage samples describe completed stages, not unfinished requests. Their
accumulated times overlap across processes and requests and must not be added
to obtain wall-clock duration. Explicit store flush counts exclude sled's
internal/background filesystem synchronizations. The storage-only baseline
does not install the subscriber and has null diagnostics. Instrumented runs
have measurement overhead and do not alone establish causality or a fix.

The existing sequenced-apply path currently passes its post-deduplication
operation count to `remote_ops_received_total`, despite that metric's
pre-deduplication description. Diagnostic reports preserve the raw value;
do not use it as a denominator for a duplicate ratio. Applied/duplicate
counts and completed batch timings remain separate observations. Correcting
the production metric is outside this diagnostic-only change.

The current Mac preflight does not pass, even with separate processes and
the approved 40-second drain: a confirmed submission took over 41 seconds.
No one-hour result is established. Do not mark the roadmap item complete
from the short healthy detector samples.

Formation is bounded to five minutes after setup exchanges and shutdown to two
minutes. The child lifetime is three minutes for the smoke test, ten minutes
for preflight and 75 minutes for the hour campaign. These outer lifecycle
bounds do not shorten the required real hour or alter detector timeouts.
Only a completed, successful hour establishes the roadmap observation;
smoke/preflight successes alone do not close it.

## Timing-sensitive storage tests

sled 0.34.7 uses a process-global concurrency-control lock: a batch in one
database can block reads in another, even when their directories are isolated.
The lost-push recovery test therefore runs its unchanged TCP scenario in a child
process of the same test binary. The parent invokes that exact test and fails
if the child fails or exceeds its 60-second total process lifetime; it kills
and reaps a timed-out child and reports bounded diagnostic output.

That lifecycle limit is not the convergence limit: the scenario still requires
100 operations, including ten omitted initial sends, to recover within the
original five-second wait after the writes. It also checks materialized values
and durable contiguous coverage with no remaining sparse receipts. An explicit
monotonic elapsed-time assertion also rejects completion after five seconds,
even if synchronous storage work delays the async polling loop. The runtime
stays current-thread, other tests remain parallel, and bulk budget tests are
neither skipped nor reduced. This isolates independent tests; it is not a
latency guarantee under arbitrary storage contention in production.

## Deterministic replay

The replay uses versioned seeded IDs/random streams, a wake-driven virtual
clock and real sled/codec paths. It compares ten runs per codec and report
hashes in three independent processes. Child execution is bounded, timed-out
processes are killed and reaped, and diagnostic reads are capped at 64 KiB.

The repeated replay itself runs in an isolated process to avoid sled's global
lock delaying unrelated real-time tests. Its 180-second total lifetime covers
ten runs per codec and three child comparisons; each comparison child retains
the existing 60-second limit. Neither lifetime changes the three-second
virtual scenario or the configured recovery deadlines.

Advance virtual time only after current work quiesces; never use arbitrary
Tokio yields or sleeps to make a trace pass. Keep deadline, delivery,
task-allocation and candidate order stable. Storage errors remain real errors;
an injected failed flush fences writes until the store is reopened.

This fixture starts from a three-node chain but lets the production reconnect
policy choose later neighbors. It is not a fixed sparse-topology certificate
or a substitute for the roadmap's 50-node, partition and detector scenarios.
