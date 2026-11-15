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
actor over real loopback TCP on macOS and Linux. Long-running scenarios are
ignored by default and require permission to spawn workers and open sockets.

Earlier 100-node experiments, including macOS preflights, did not meet the
expected performance goals. Performance and scalability work is tracked under
v0.1.11 in the [roadmap](../../../../../docs/nx-site/src/content/docs/roadmap/index.md).
The 50-node workflow below does not establish 100-node capacity.

Run the preflight before the full-hour scenario:

```bash
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::detector_soak::detector_50_nodes_preflight_json \
  -- --ignored --exact --nocapture
```

Run the one-hour campaign with:

```bash
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::detector_soak::detector_50_nodes_one_hour_json \
  -- --ignored --exact --nocapture
```

For a storage-only scenario:

```bash
cargo test --release --locked -p nx-core --lib \
  sync_manager::manager::tests::detector_soak::storage_100_processes_preflight \
  -- --ignored --exact --nocapture
```

Run resource-intensive scenarios separately from the workspace suite. Keep
worker ownership, bounded shutdown and production detector settings intact.

## Storage and wire compatibility

Keep the historical fixtures immutable. Their generation instructions are in
[v0.1.0](../fixtures/v0_1_0/README.md) and
[v0.1.5](../fixtures/v0_1_5/README.md).

Run the migration, recovery and codec tests with:

```bash
cargo test -p nx-core baseline_
cargo test -p nx-core v015
cargo test -p nx-core sequence_v1_migration
cargo test -p nx-core migrates_fixture_generated_by_v0_1_0
cargo test -p nx-net
```

The previous-version process test requires local sockets, a
`wasm32-unknown-unknown` build of `examples/distributed_counter`, the current
`nx-cli` and `NUMAX_PREVIOUS_NX_BIN`. Follow the `cli-smoke` job in
[CI](../../../../../.github/workflows/ci.yml) for the pinned v0.1.5 binary and
setup:

```bash
cargo test -p nx-cli --test multiprocess_smoke \
  different_protocol_versions_reject_connection_without_exchanging_ops \
  -- --exact --ignored
```

Use disposable stores, never an operator's datastore. Process tests require
permission to spawn and terminate children.

## Recovery under continued writes

On Linux with unprivileged user/network namespaces, `ip`, `tc`, `nsenter`,
`nstat`, `jq`, GNU `timeout`, `tar` and `sha256sum`, use new artifact directories:

```bash
bash scripts/check-real-packet-loss.sh --cluster-load-smoke /tmp/numax-load-smoke-NEW_RUN
bash scripts/check-real-packet-loss.sh --cluster-load /tmp/numax-load-NEW_RUN
```

These modes use `packet_loss.rs` and private network namespaces. Run the smoke
before the full scenario. They are separate from the primary partition tests,
which stop writes before rejoin. Do not modify host networking or use an
existing artifact directory. Keep generated artifacts outside this README.

## Timing-sensitive storage tests

sled 0.34.7 uses a process-global concurrency-control lock: a batch in one
database can block reads in another, even when their directories are isolated.
Use the existing bounded child-process helper when isolating timing-sensitive
scenarios. Preserve their runtime flavor, convergence deadlines and workload;
do not compensate for interference by weakening assertions or increasing
timeouts. Always kill and reap timed-out children.

## Deterministic replay

`determinism.rs` uses seeded IDs/random streams, a wake-driven virtual clock
and real sled/codec paths in isolated child processes.
Advance virtual time only after current work quiesces; never use arbitrary
Tokio yields or sleeps to make a trace pass. Keep deadline, delivery,
task-allocation and candidate order stable. Storage errors remain real errors;
an injected failed flush fences writes until the store is reopened.
