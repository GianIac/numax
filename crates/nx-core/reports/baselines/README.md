Baseline reports for `nx-core` load benchmarks.

These historical files use `report_schema_version = 1`. They are intentionally
separate from `reports/load/`, which remains historical benchmark output.

The three-node smoke reference from commit `03fd9e6` measured enqueue/in-memory
acceptance, not the current durable local submission acknowledgement. It remains
unchanged and is **not comparable** with the corrected benchmark. The comparator
rejects schema-1 reports, including this reference, rather than silently treating
them as durable-ack measurements. A new baseline requires separate approval and
calibration; changing a historical report's version or profile is not calibration.

## Corrected three-node report contract

The benchmark now emits `report_schema_version = 2` with
`profile.acceptance = "local-durable-flush-v1"`. Successful operation latency and
throughput include `submit_gcounter_increment` completion, covering the local
flush acknowledgement; neither metric proves remote durability.
`load_duration_secs` covers the producer phase. `total_duration_secs` includes
that phase, final convergence/observation, and all bounded manager shutdown
attempts, including failed or timed-out cleanup. It excludes setup and report
serialization/output.

- `target_ops_total` is the unchanged configured workload.
- `attempted_ops_total` counts submissions begun; `ops_total` counts successful
  acknowledgements only.
- `errors_total` counts failed or uncertain attempts. `uncertain_ops_total` is
  the subset without a proven rollback (currently all errors, timeouts and
  interrupted in-flight submissions). Error acknowledgements are not assumed
  to mean that no durable state was written.
- `definite_errors_total = errors_total - uncertain_ops_total` explicitly
  separates proven failures from uncertain outcomes, without changing the
  schema-2 aggregate `errors_total`. The accounting invariant is
  `attempted_ops_total = ops_total + definite_errors_total + uncertain_ops_total`.
  A generic acceptance error does not prove a missing commit and therefore
  remains uncertain, not a definite failure.
- `unattempted_ops_total` records missed workload, separately from attempts
  with an unknown outcome.
- `expected_counter` equals `ops_total`, never successes minus failures. It is
  `null` when any attempt is uncertain; `converged` is then false even if the
  observed counters happen to match.
- `cleanup_errors_total` and bounded `error_details` are included after cleanup.
  At most 16 diagnostic samples of 1024 characters are retained; counts remain
  exact. Producer progress survives task failure. Timed-out producers are
  aborted **and joined**, and all owned managers receive a bounded shutdown
  attempt, including partially failed setup.
- `report_complete` is true only for the entire acknowledged workload, exact
  convergence, and clean teardown with no errors. Failed runs still print/write
  a report after cleanup and exit nonzero. An unavailable output path can prevent
  saving the file, but cleanup has already been attempted.

The comparator accepts only complete schema-2 reports with matching profiles,
zero failed/uncertain/missed work, clean cleanup and counters for every node.
Invalid inputs fail in both shadow and blocking modes, before median aggregation.
The benchmark retains its workload, rate, queue/retention budgets, send and
shutdown deadlines, and convergence deadline; no batching is introduced.
Isolated `benchmark-store-*` directories are created under the working directory
and removed by their owned directory guards after teardown.

Focused regression checks (the benchmark itself uses `harness = false`):

```sh
cargo test -p nx-core --test three_node_sync_load
node --test scripts/compare-benchmark-report.test.mjs
```

Naming convention:

- `<scenario-profile>.json` for committed baselines;
- `*-smoke.json` for short PR or CI scenarios;
- longer release-gate scenarios can be added beside them once they are stable.

Legacy baselines normalized from historical load reports may have a null `resources.rss_bytes`. A baseline used by a blocking RSS gate must come from the calibration workflow and contain a finite RSS value.

## Regression gate

Pull-request CI runs the three-node smoke profile three times on `ubuntu-24.04` and compares the median p99 latency, throughput, and RSS against `three-node-sync-smoke.json`. Raw runs, the median report, and the comparison summary are uploaded as the `benchmark-regression-report` artifact.

The gate is configured in `blocking` mode. Until a separately reviewed durable-ack
baseline exists, comparison against the retained historical reference fails as
an incompatible report, not a measured performance regression. Once compatible
references exist, regressions beyond the configured thresholds fail the
pull-request workflow; invalid reports and benchmark correctness failures remain
separate hard failures.

## Baseline calibration

Run the manual **Benchmark Baseline Calibration** workflow. 
It executes five identical benchmark runs, writes their median to `reports/baseline-candidate/three-node-sync-smoke.json`, and uploads the candidate together with every raw report. The workflow never modifies the repository automatically.

The existing workflow still passes the schema-1 reference to the comparator,
so its comparison/aggregation step also fails until a separately approved
calibration update supplies a compatible reference. This correction does not
reconfigure CI, generate measurements, or replace committed baselines.

Before replacing the committed baseline:

1. inspect all five raw reports for outliers and errors;
2. verify that the candidate has a finite `resources.rss_bytes`;
3. review the p99, throughput, and RSS deltas in the workflow summary;
4. commit the candidate through a normal pull request.

The blocking thresholds are 10% plus 0.1 ms for p99, 5% for throughput, and 10% plus 16 MiB for RSS. Both the relative and absolute threshold must be exceeded for p99 or RSS to count as a regression.
