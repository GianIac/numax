import assert from 'node:assert/strict';
import test from 'node:test';

import { compareReports } from './compare-benchmark-report.mjs';

const baseline = {
  report_schema_version: 1,
  crate: 'nx-core',
  benchmark: 'three_node_sync_load',
  scenario: 'multi-node-sync-gcounter',
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

test('accepts metrics within thresholds', () => {
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

test('detects p99 throughput and rss regressions', () => {
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
    ['latency_ms.p99', 'ops_sec_avg', 'resources.rss_bytes']
  );
});

test('skips rss when either side is null', () => {
  const result = compareReports(
    {
      ...baseline,
      resources: {
        rss_bytes: null,
      },
    },
    baseline
  );

  const rss = result.comparisons.find((comparison) => comparison.metric === 'resources.rss_bytes');
  assert.equal(rss.status, 'skipped');
  assert.equal(result.regressions.length, 0);
});

test('rejects mismatched scenarios', () => {
  assert.throws(
    () =>
      compareReports(baseline, {
        ...baseline,
        scenario: 'different',
      }),
    /scenario mismatch/
  );
});

test('rejects mismatched workload profiles', () => {
  assert.throws(
    () =>
      compareReports(baseline, {
        ...baseline,
        profile: {
          ...baseline.profile,
          nodes: 10,
        },
      }),
    /profile mismatch/
  );
});
