import assert from "node:assert/strict";
import test from "node:test";

import {
  aggregateReports,
  compareReports,
  compareReportSet,
} from "./compare-benchmark-report.mjs";

const baseline = {
  report_schema_version: 1,
  crate: "nx-core",
  benchmark: "three_node_sync_load",
  scenario: "multi-node-sync-gcounter",
  profile: {
    nodes: 3,
    duration_secs: 10,
    target_ops_sec_per_node: 1000,
    anti_entropy_interval_secs: 80,
  },
  ops_sec_avg: 1000,
  resources: {
    rss_bytes: 1000,
  },
  latency_ms: {
    p99: 10,
  },
};

test("accepts metrics within thresholds", () => {
  const result = compareReports(baseline, {
    ...baseline,
    ops_sec_avg: 960,
    resources: {
      rss_bytes: 1040,
    },
    latency_ms: {
      p99: 10.4,
    },
  });

  assert.equal(result.regressions.length, 0);
});

test("detects p99 throughput and rss regressions", () => {
  const result = compareReports(baseline, {
    ...baseline,
    ops_sec_avg: 900,
    resources: {
      rss_bytes: 1100,
    },
    latency_ms: {
      p99: 11,
    },
  });

  assert.deepEqual(
    result.regressions.map((regression) => regression.metric),
    ["latency_ms.p99", "ops_sec_avg", "resources.rss_bytes"],
  );
});

test("skips rss while the committed baseline is null", () => {
  const result = compareReports(
    {
      ...baseline,
      resources: {
        rss_bytes: null,
      },
    },
    baseline,
  );

  const rss = result.comparisons.find(
    (comparison) => comparison.metric === "resources.rss_bytes",
  );
  assert.equal(rss.status, "skipped");
  assert.equal(result.regressions.length, 0);
});

test("rejects mismatched scenarios", () => {
  assert.throws(
    () =>
      compareReports(baseline, {
        ...baseline,
        scenario: "different",
      }),
    /scenario mismatch/,
  );
});

test("rejects mismatched workload profiles", () => {
  assert.throws(
    () =>
      compareReports(baseline, {
        ...baseline,
        profile: {
          ...baseline.profile,
          nodes: 10,
        },
      }),
    /profile mismatch/,
  );
});

test("compares the median of multiple current reports", () => {
  const result = compareReportSet(baseline, [
    {
      ...baseline,
      ops_sec_avg: 950,
      resources: { rss_bytes: 1040 },
      latency_ms: { p99: 10.4 },
    },
    {
      ...baseline,
      ops_sec_avg: 100,
      resources: { rss_bytes: 9000 },
      latency_ms: { p99: 100 },
    },
    {
      ...baseline,
      ops_sec_avg: 970,
      resources: { rss_bytes: 1020 },
      latency_ms: { p99: 10.2 },
    },
  ]);

  assert.equal(result.runs, 3);
  assert.equal(result.aggregate.ops_sec_avg, 950);
  assert.equal(result.aggregate.resources.rss_bytes, 1040);
  assert.equal(result.aggregate.latency_ms.p99, 10.4);
  assert.equal(result.regressions.length, 0);
});

test("requires both relative and absolute thresholds to flag a regression", () => {
  const result = compareReports(
    baseline,
    {
      ...baseline,
      ops_sec_avg: 1000,
      resources: { rss_bytes: 1100 },
      latency_ms: { p99: 11 },
    },
    {
      thresholds: { p99: 5, throughput: 5, rss: 5 },
      minimumDeltas: { p99: 2, rss: 200 },
    },
  );

  assert.equal(result.regressions.length, 0);
  assert.equal(result.comparisons[0].delta, 1);
  assert.equal(result.comparisons[2].delta, 100);
});

test("aggregates latency fields and records aggregation metadata", () => {
  const aggregate = aggregateReports([
    { ...baseline, latency_ms: { p50: 1, p99: 9 } },
    { ...baseline, latency_ms: { p50: 3, p99: 11 } },
    { ...baseline, latency_ms: { p50: 2, p99: 10 } },
  ]);

  assert.deepEqual(aggregate.latency_ms, { p50: 2, p99: 10 });
  assert.deepEqual(aggregate.aggregation, { method: "median", runs: 3 });
});

test("rejects a missing current RSS once the baseline has RSS", () => {
  assert.throws(
    () =>
      compareReports(baseline, {
        ...baseline,
        resources: { rss_bytes: null },
      }),
    /current resources\.rss_bytes is required/,
  );
});
