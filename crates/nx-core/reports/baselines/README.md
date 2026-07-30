Baseline reports for `nx-core` load benchmarks.

These files use `report_schema_version = 1` and are the inputs future
regression checks compare against. They are intentionally separate from
`reports/load/`, which remains historical benchmark output.

Naming convention:

- `<scenario-profile>.json` for committed baselines;
- `*-smoke.json` for short PR or shadow-CI scenarios;
- longer release-gate scenarios can be added beside them once they are stable.

Initial baselines are normalized from existing committed load reports, so
`resources.rss_bytes` is `null` until fresh v1 reports replace them.

## Regression gate

Pull-request CI runs the three-node smoke profile three times on
`ubuntu-24.04` and compares the median p99 latency, throughput, and RSS against
`three-node-sync-smoke.json`. Raw runs, the median report, and the comparison
summary are uploaded as the `benchmark-regression-report` artifact.

The gate starts in `shadow` mode. Set the repository variable
`NUMAX_BENCH_GATE_MODE=blocking` only after committing a reviewed CI-generated
baseline with a non-null RSS value.

## Baseline calibration

Run the manual **Benchmark Baseline Calibration** workflow. It executes five
identical benchmark runs, writes their median to
`reports/baseline-candidate/three-node-sync-smoke.json`, and uploads the
candidate together with every raw report. The workflow never modifies the
repository automatically.

Before replacing the committed baseline:

1. inspect all five raw reports for outliers and errors;
2. verify that the candidate has a finite `resources.rss_bytes`;
3. review the p99, throughput, and RSS deltas in the workflow summary;
4. commit the candidate through a normal pull request.

The initial shadow thresholds are 10% plus 0.1 ms for p99, 5% for throughput,
and 10% plus 16 MiB for RSS. Both the relative and absolute threshold must be
exceeded for p99 or RSS to count as a regression.
