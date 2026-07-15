#!/usr/bin/env node
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { isDeepStrictEqual } from 'node:util';

const DEFAULT_THRESHOLDS = {
  p99: 5,
  throughput: 5,
  rss: 5,
};

export function compareReports(baseline, current, options = {}) {
  const thresholds = { ...DEFAULT_THRESHOLDS, ...(options.thresholds || {}) };
  const comparisons = [];

  ensureReportShape('baseline', baseline);
  ensureReportShape('current', current);

  if (baseline.scenario !== current.scenario) {
    throw new Error(`scenario mismatch: baseline=${baseline.scenario} current=${current.scenario}`);
  }
  if (baseline.benchmark !== current.benchmark) {
    throw new Error(`benchmark mismatch: baseline=${baseline.benchmark} current=${current.benchmark}`);
  }
  if (baseline.crate !== current.crate) {
    throw new Error(`crate mismatch: baseline=${baseline.crate} current=${current.crate}`);
  }
  if (!isDeepStrictEqual(baseline.profile, current.profile)) {
    throw new Error(
      `profile mismatch: baseline=${JSON.stringify(baseline.profile)} current=${JSON.stringify(
        current.profile
      )}`
    );
  }

  comparisons.push(
    compareHigherIsWorse({
      metric: 'latency_ms.p99',
      baseline: requiredNumber(baseline.latency_ms?.p99, 'baseline latency_ms.p99'),
      current: requiredNumber(current.latency_ms?.p99, 'current latency_ms.p99'),
      thresholdPercent: thresholds.p99,
    })
  );

  comparisons.push(
    compareLowerIsWorse({
      metric: 'ops_sec_avg',
      baseline: requiredNumber(baseline.ops_sec_avg, 'baseline ops_sec_avg'),
      current: requiredNumber(current.ops_sec_avg, 'current ops_sec_avg'),
      thresholdPercent: thresholds.throughput,
    })
  );

  const baselineRss = optionalNumber(baseline.resources?.rss_bytes, 'baseline resources.rss_bytes');
  const currentRss = optionalNumber(current.resources?.rss_bytes, 'current resources.rss_bytes');
  if (baselineRss === null || currentRss === null) {
    comparisons.push({
      metric: 'resources.rss_bytes',
      baseline: baselineRss,
      current: currentRss,
      thresholdPercent: thresholds.rss,
      deltaPercent: null,
      status: 'skipped',
      reason: 'rss_bytes is null in baseline or current report',
    });
  } else {
    comparisons.push(
      compareHigherIsWorse({
        metric: 'resources.rss_bytes',
        baseline: baselineRss,
        current: currentRss,
        thresholdPercent: thresholds.rss,
      })
    );
  }

  return {
    scenario: current.scenario,
    benchmark: current.benchmark,
    comparisons,
    regressions: comparisons.filter((comparison) => comparison.status === 'regression'),
  };
}

export function formatComparisonResult(result, mode = 'shadow') {
  const lines = [
    `Benchmark regression check (${mode})`,
    `scenario: ${result.scenario}`,
    `benchmark: ${result.benchmark}`,
  ];

  for (const comparison of result.comparisons) {
    if (comparison.status === 'skipped') {
      lines.push(
        `- ${comparison.metric}: skipped (${comparison.reason}; baseline=${formatValue(
          comparison.baseline
        )}, current=${formatValue(comparison.current)})`
      );
      continue;
    }

    lines.push(
      `- ${comparison.metric}: ${comparison.status}; baseline=${formatValue(
        comparison.baseline
      )}, current=${formatValue(comparison.current)}, delta=${comparison.deltaPercent.toFixed(
        2
      )}%, threshold=${comparison.thresholdPercent.toFixed(2)}%`
    );
  }

  return lines.join('\n');
}

function compareHigherIsWorse({ metric, baseline, current, thresholdPercent }) {
  if (baseline <= 0) {
    throw new Error(`${metric} baseline must be greater than zero`);
  }
  const deltaPercent = ((current - baseline) / baseline) * 100;
  return {
    metric,
    baseline,
    current,
    thresholdPercent,
    deltaPercent,
    status: deltaPercent > thresholdPercent ? 'regression' : 'ok',
  };
}

function compareLowerIsWorse({ metric, baseline, current, thresholdPercent }) {
  if (baseline <= 0) {
    throw new Error(`${metric} baseline must be greater than zero`);
  }
  const deltaPercent = ((baseline - current) / baseline) * 100;
  return {
    metric,
    baseline,
    current,
    thresholdPercent,
    deltaPercent,
    status: deltaPercent > thresholdPercent ? 'regression' : 'ok',
  };
}

function ensureReportShape(label, report) {
  if (report.report_schema_version !== 1) {
    throw new Error(`${label} report_schema_version must be 1`);
  }
  if (!report.scenario) {
    throw new Error(`${label} scenario is required`);
  }
  if (!report.benchmark) {
    throw new Error(`${label} benchmark is required`);
  }
  if (!report.crate) {
    throw new Error(`${label} crate is required`);
  }
  if (!report.profile || typeof report.profile !== 'object' || Array.isArray(report.profile)) {
    throw new Error(`${label} profile is required`);
  }
}

function requiredNumber(value, label) {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`${label} must be a finite number`);
  }
  return value;
}

function optionalNumber(value, label) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`${label} must be a finite number or null`);
  }
  return value;
}

function formatValue(value) {
  return value === null ? 'null' : String(value);
}

function parseArgs(argv) {
  const args = {
    mode: 'shadow',
    thresholds: { ...DEFAULT_THRESHOLDS },
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    switch (arg) {
      case '--baseline':
        args.baselinePath = nextArg(argv, ++index, arg);
        break;
      case '--current':
        args.currentPath = nextArg(argv, ++index, arg);
        break;
      case '--mode':
        args.mode = nextArg(argv, ++index, arg);
        break;
      case '--p99-threshold':
        args.thresholds.p99 = parseThreshold(nextArg(argv, ++index, arg), arg);
        break;
      case '--throughput-threshold':
        args.thresholds.throughput = parseThreshold(nextArg(argv, ++index, arg), arg);
        break;
      case '--rss-threshold':
        args.thresholds.rss = parseThreshold(nextArg(argv, ++index, arg), arg);
        break;
      case '--help':
      case '-h':
        printHelp();
        process.exit(0);
        break;
      default:
        throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (!args.baselinePath) {
    throw new Error('--baseline is required');
  }
  if (!args.currentPath) {
    throw new Error('--current is required');
  }
  if (!['shadow', 'blocking'].includes(args.mode)) {
    throw new Error('--mode must be shadow or blocking');
  }

  return args;
}

function nextArg(argv, index, name) {
  const value = argv[index];
  if (!value) {
    throw new Error(`missing value for ${name}`);
  }
  return value;
}

function parseThreshold(value, name) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative number`);
  }
  return parsed;
}

function readJson(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

function printHelp() {
  console.log(`Compare a benchmark report against a committed baseline.

Options:
  --baseline PATH              Baseline JSON report
  --current PATH               Current JSON report
  --mode shadow|blocking       Print regressions only, or fail on regressions (default: shadow)
  --p99-threshold PERCENT      Allowed p99 increase (default: 5)
  --throughput-threshold PERCENT
                               Allowed throughput decrease (default: 5)
  --rss-threshold PERCENT      Allowed RSS increase (default: 5)
`);
}

function main() {
  try {
    const args = parseArgs(process.argv.slice(2));
    const result = compareReports(readJson(args.baselinePath), readJson(args.currentPath), {
      thresholds: args.thresholds,
    });
    const output = formatComparisonResult(result, args.mode);
    console.log(output);

    if (result.regressions.length > 0) {
      const annotation = args.mode === 'blocking' ? 'error' : 'warning';
      console.log(`::${annotation}::${result.regressions.length} benchmark regression(s) detected`);
      if (args.mode === 'blocking') {
        process.exit(1);
      }
    }
  } catch (error) {
    console.error(`benchmark comparison failed: ${error.message}`);
    process.exit(2);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  main();
}
