import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

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

const comparatorPath = fileURLToPath(
  new URL("./compare-benchmark-report.mjs", import.meta.url),
);

function reportWith({
  opsSec = baseline.ops_sec_avg,
  rssBytes = baseline.resources.rss_bytes,
  p99 = baseline.latency_ms.p99,
} = {}) {
  return {
    ...baseline,
    profile: { ...baseline.profile },
    ops_sec_avg: opsSec,
    resources: { rss_bytes: rssBytes },
    latency_ms: { p99 },
  };
}

function writeReport(directory, name, report) {
  const path = join(directory, name);
  writeFileSync(path, `${JSON.stringify(report)}\n`);
  return path;
}

function runComparator(args) {
  const env = { ...process.env };
  delete env.NODE_TEST_CONTEXT;
  const result = spawnSync(process.execPath, [comparatorPath, ...args], {
    encoding: "utf8",
    env,
  });
  if (result.error) {
    throw result.error;
  }
  return result;
}

function withTemporaryDirectory(testContext) {
  const directory = mkdtempSync(join(tmpdir(), "numax-benchmark-comparator-"));
  testContext.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

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

test("CLI aggregates repeated current reports and writes the median", (t) => {
  const directory = withTemporaryDirectory(t);
  const baselinePath = writeReport(directory, "baseline.json", baseline);
  const currentPaths = [
    writeReport(
      directory,
      "current-1.json",
      reportWith({ opsSec: 980, rssBytes: 1020, p99: 10.2 }),
    ),
    writeReport(
      directory,
      "current-2.json",
      reportWith({ opsSec: 960, rssBytes: 1040, p99: 10.4 }),
    ),
    writeReport(
      directory,
      "current-3.json",
      reportWith({ opsSec: 970, rssBytes: 1030, p99: 10.3 }),
    ),
  ];
  const aggregatePath = join(directory, "aggregate", "median.json");

  const result = runComparator([
    "--baseline",
    baselinePath,
    ...currentPaths.flatMap((path) => ["--current", path]),
    "--write-aggregate",
    aggregatePath,
    "--mode",
    "blocking",
  ]);

  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.match(result.stdout, /current runs: 3 \(median\)/);
  const aggregate = JSON.parse(readFileSync(aggregatePath, "utf8"));
  assert.equal(aggregate.ops_sec_avg, 970);
  assert.equal(aggregate.resources.rss_bytes, 1030);
  assert.equal(aggregate.latency_ms.p99, 10.3);
  assert.deepEqual(aggregate.aggregation, { method: "median", runs: 3 });
});

test("CLI blocking mode exits 1 when metrics regress", (t) => {
  const directory = withTemporaryDirectory(t);
  const baselinePath = writeReport(directory, "baseline.json", baseline);
  const currentPath = writeReport(
    directory,
    "current.json",
    reportWith({ opsSec: 900, rssBytes: 1100, p99: 11 }),
  );

  const result = runComparator([
    "--baseline",
    baselinePath,
    "--current",
    currentPath,
    "--mode",
    "blocking",
  ]);

  assert.equal(result.status, 1, result.stderr || result.stdout);
  assert.match(result.stdout, /3 benchmark regression\(s\) detected/);
  assert.match(result.stdout, /::error::/);
});

test("CLI shadow mode reports regressions without failing", (t) => {
  const directory = withTemporaryDirectory(t);
  const baselinePath = writeReport(directory, "baseline.json", baseline);
  const currentPath = writeReport(
    directory,
    "current.json",
    reportWith({ opsSec: 900, rssBytes: 1100, p99: 11 }),
  );

  const result = runComparator([
    "--baseline",
    baselinePath,
    "--current",
    currentPath,
    "--mode",
    "shadow",
  ]);

  assert.equal(result.status, 0, result.stderr || result.stdout);
  assert.match(result.stdout, /3 benchmark regression\(s\) detected/);
  assert.match(result.stdout, /::warning::/);
});

test("CLI exits 2 for malformed JSON input", (t) => {
  const directory = withTemporaryDirectory(t);
  const baselinePath = writeReport(directory, "baseline.json", baseline);
  const malformedPath = join(directory, "malformed.json");
  writeFileSync(malformedPath, "{ not valid JSON");

  const result = runComparator([
    "--baseline",
    baselinePath,
    "--current",
    malformedPath,
    "--mode",
    "blocking",
  ]);

  assert.equal(result.status, 2, result.stderr || result.stdout);
  assert.match(result.stderr, /benchmark comparison failed:/);
});
