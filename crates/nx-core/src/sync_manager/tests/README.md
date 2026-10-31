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
