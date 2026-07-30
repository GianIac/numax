#!/usr/bin/env node
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";

const DEFAULT_THRESHOLDS = {
  p99: 5,
  throughput: 5,
  rss: 5,
};

const DEFAULT_MINIMUM_DELTAS = {
  p99: 0,
  rss: 0,
};

export function compareReports(baseline, current, options = {}) {
  return compareReportSet(baseline, [current], options);
}

export function compareReportSet(baseline, currentReports, options = {}) {
  const thresholds = { ...DEFAULT_THRESHOLDS, ...(options.thresholds || {}) };
  const minimumDeltas = {
    ...DEFAULT_MINIMUM_DELTAS,
    ...(options.minimumDeltas || {}),
  };
  const comparisons = [];

  ensureReportShape("baseline", baseline);
  const current = aggregateReports(currentReports);
  ensureCompatibleReports("baseline", baseline, "current aggregate", current);

  comparisons.push(
    compareHigherIsWorse({
      metric: "latency_ms.p99",
      baseline: requiredNumber(
        baseline.latency_ms?.p99,
        "baseline latency_ms.p99",
      ),
      current: requiredNumber(
        current.latency_ms?.p99,
        "current latency_ms.p99",
      ),
      thresholdPercent: thresholds.p99,
      minimumDelta: minimumDeltas.p99,
    }),
  );

  comparisons.push(
    compareLowerIsWorse({
      metric: "ops_sec_avg",
      baseline: requiredNumber(baseline.ops_sec_avg, "baseline ops_sec_avg"),
      current: requiredNumber(current.ops_sec_avg, "current ops_sec_avg"),
      thresholdPercent: thresholds.throughput,
      minimumDelta: 0,
    }),
  );

  const baselineRss = optionalNumber(
    baseline.resources?.rss_bytes,
    "baseline resources.rss_bytes",
  );
  const currentRss = optionalNumber(
    current.resources?.rss_bytes,
    "current resources.rss_bytes",
  );
  if (baselineRss === null) {
    comparisons.push({
      metric: "resources.rss_bytes",
      baseline: null,
      current: currentRss,
      thresholdPercent: thresholds.rss,
      minimumDelta: minimumDeltas.rss,
      delta: null,
      deltaPercent: null,
      status: "skipped",
      reason: "rss_bytes is null in the committed baseline",
    });
  } else {
    if (currentRss === null) {
      throw new Error(
        "current resources.rss_bytes is required when the baseline RSS is present",
      );
    }
    comparisons.push(
      compareHigherIsWorse({
        metric: "resources.rss_bytes",
        baseline: baselineRss,
        current: currentRss,
        thresholdPercent: thresholds.rss,
        minimumDelta: minimumDeltas.rss,
      }),
    );
  }

  return {
    scenario: current.scenario,
    benchmark: current.benchmark,
    runs: currentReports.length,
    aggregate: current,
    comparisons,
    regressions: comparisons.filter(
      (comparison) => comparison.status === "regression",
    ),
  };
}

export function aggregateReports(reports) {
  if (!Array.isArray(reports) || reports.length === 0) {
    throw new Error("at least one current report is required");
  }

  reports.forEach((report, index) =>
    ensureReportShape(`current[${index}]`, report),
  );
  const first = reports[0];
  reports.slice(1).forEach((report, index) => {
    ensureCompatibleReports(
      "current[0]",
      first,
      `current[${index + 1}]`,
      report,
    );
  });

  const aggregate = JSON.parse(JSON.stringify(first));
  aggregate.ops_sec_avg = median(
    reports.map((report, index) =>
      requiredNumber(report.ops_sec_avg, `current[${index}] ops_sec_avg`),
    ),
  );

  const rssValues = reports.map((report, index) =>
    optionalNumber(
      report.resources?.rss_bytes,
      `current[${index}] resources.rss_bytes`,
    ),
  );
  aggregate.resources = {
    ...aggregate.resources,
    rss_bytes: rssValues.some((value) => value === null)
      ? null
      : median(rssValues),
  };

  const latencyKeys = new Set(
    reports.flatMap((report) => Object.keys(report.latency_ms || {})),
  );
  aggregate.latency_ms = { ...aggregate.latency_ms };
  for (const key of latencyKeys) {
    aggregate.latency_ms[key] = median(
      reports.map((report, index) =>
        requiredNumber(
          report.latency_ms?.[key],
          `current[${index}] latency_ms.${key}`,
        ),
      ),
    );
  }

  for (const field of [
    "load_duration_secs",
    "total_duration_secs",
    "convergence_wait_secs",
  ]) {
    if (reports.every((report) => typeof report[field] === "number")) {
      aggregate[field] = median(reports.map((report) => report[field]));
    }
  }

  aggregate.aggregation = {
    method: "median",
    runs: reports.length,
  };
  return aggregate;
}

export function formatComparisonResult(result, mode = "shadow") {
  const lines = [
    `Benchmark regression check (${mode})`,
    `scenario: ${result.scenario}`,
    `benchmark: ${result.benchmark}`,
    `current runs: ${result.runs} (median)`,
  ];

  for (const comparison of result.comparisons) {
    if (comparison.status === "skipped") {
      lines.push(
        `- ${comparison.metric}: skipped (${comparison.reason}; baseline=${formatValue(
          comparison.baseline,
        )}, current=${formatValue(comparison.current)})`,
      );
      continue;
    }

    const minimumDelta =
      comparison.minimumDelta > 0
        ? `, minimum_delta=${comparison.minimumDelta}`
        : "";
    lines.push(
      `- ${comparison.metric}: ${comparison.status}; baseline=${formatValue(
        comparison.baseline,
      )}, current=${formatValue(comparison.current)}, delta=${comparison.deltaPercent.toFixed(
        2,
      )}%, threshold=${comparison.thresholdPercent.toFixed(2)}%${minimumDelta}`,
    );
  }

  return lines.join("\n");
}

function compareHigherIsWorse({
  metric,
  baseline,
  current,
  thresholdPercent,
  minimumDelta,
}) {
  if (baseline <= 0) {
    throw new Error(`${metric} baseline must be greater than zero`);
  }
  const delta = current - baseline;
  const deltaPercent = (delta / baseline) * 100;
  return comparisonResult({
    metric,
    baseline,
    current,
    thresholdPercent,
    minimumDelta,
    delta,
    deltaPercent,
  });
}

function compareLowerIsWorse({
  metric,
  baseline,
  current,
  thresholdPercent,
  minimumDelta,
}) {
  if (baseline <= 0) {
    throw new Error(`${metric} baseline must be greater than zero`);
  }
  const delta = baseline - current;
  const deltaPercent = (delta / baseline) * 100;
  return comparisonResult({
    metric,
    baseline,
    current,
    thresholdPercent,
    minimumDelta,
    delta,
    deltaPercent,
  });
}

function comparisonResult({
  metric,
  baseline,
  current,
  thresholdPercent,
  minimumDelta,
  delta,
  deltaPercent,
}) {
  return {
    metric,
    baseline,
    current,
    thresholdPercent,
    minimumDelta,
    delta,
    deltaPercent,
    status:
      deltaPercent > thresholdPercent && delta > minimumDelta
        ? "regression"
        : "ok",
  };
}

function ensureCompatibleReports(leftLabel, left, rightLabel, right) {
  if (left.scenario !== right.scenario) {
    throw new Error(
      `scenario mismatch: ${leftLabel}=${left.scenario} ${rightLabel}=${right.scenario}`,
    );
  }
  if (left.benchmark !== right.benchmark) {
    throw new Error(
      `benchmark mismatch: ${leftLabel}=${left.benchmark} ${rightLabel}=${right.benchmark}`,
    );
  }
  if (left.crate !== right.crate) {
    throw new Error(
      `crate mismatch: ${leftLabel}=${left.crate} ${rightLabel}=${right.crate}`,
    );
  }
  if (!isDeepStrictEqual(left.profile, right.profile)) {
    throw new Error(
      `profile mismatch: ${leftLabel}=${JSON.stringify(
        left.profile,
      )} ${rightLabel}=${JSON.stringify(right.profile)}`,
    );
  }
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
  if (
    !report.profile ||
    typeof report.profile !== "object" ||
    Array.isArray(report.profile)
  ) {
    throw new Error(`${label} profile is required`);
  }
}

function requiredNumber(value, label) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${label} must be a finite number`);
  }
  return value;
}

function optionalNumber(value, label) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${label} must be a finite number or null`);
  }
  return value;
}

function median(values) {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1] + sorted[middle]) / 2
    : sorted[middle];
}

function formatValue(value) {
  return value === null ? "null" : String(value);
}

function parseArgs(argv) {
  const args = {
    currentPaths: [],
    mode: "shadow",
    thresholds: { ...DEFAULT_THRESHOLDS },
    minimumDeltas: { ...DEFAULT_MINIMUM_DELTAS },
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    switch (arg) {
      case "--baseline":
        args.baselinePath = nextArg(argv, ++index, arg);
        break;
      case "--current":
        args.currentPaths.push(nextArg(argv, ++index, arg));
        break;
      case "--write-aggregate":
        args.aggregatePath = nextArg(argv, ++index, arg);
        break;
      case "--mode":
        args.mode = nextArg(argv, ++index, arg);
        break;
      case "--p99-threshold":
        args.thresholds.p99 = parseThreshold(nextArg(argv, ++index, arg), arg);
        break;
      case "--throughput-threshold":
        args.thresholds.throughput = parseThreshold(
          nextArg(argv, ++index, arg),
          arg,
        );
        break;
      case "--rss-threshold":
        args.thresholds.rss = parseThreshold(nextArg(argv, ++index, arg), arg);
        break;
      case "--p99-min-delta-ms":
        args.minimumDeltas.p99 = parseThreshold(
          nextArg(argv, ++index, arg),
          arg,
        );
        break;
      case "--rss-min-delta-bytes":
        args.minimumDeltas.rss = parseThreshold(
          nextArg(argv, ++index, arg),
          arg,
        );
        break;
      case "--help":
      case "-h":
        printHelp();
        process.exit(0);
        break;
      default:
        throw new Error(`unknown argument: ${arg}`);
    }
  }

  if (!args.baselinePath) {
    throw new Error("--baseline is required");
  }
  if (args.currentPaths.length === 0) {
    throw new Error("at least one --current is required");
  }
  if (!["shadow", "blocking"].includes(args.mode)) {
    throw new Error("--mode must be shadow or blocking");
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
  return JSON.parse(readFileSync(path, "utf8"));
}

function printHelp() {
  console.log(`Compare one or more benchmark reports against a committed baseline.

Options:
  --baseline PATH              Baseline JSON report
  --current PATH               Current report; repeat to compare their median
  --write-aggregate PATH       Write the median aggregate report
  --mode shadow|blocking       Warn, or fail on regressions (default: shadow)
  --p99-threshold PERCENT      Allowed median p99 increase (default: 5)
  --p99-min-delta-ms VALUE     Minimum absolute p99 increase (default: 0)
  --throughput-threshold PERCENT
                               Allowed median throughput decrease (default: 5)
  --rss-threshold PERCENT      Allowed median RSS increase (default: 5)
  --rss-min-delta-bytes VALUE  Minimum absolute RSS increase (default: 0)
`);
}

function main() {
  try {
    const args = parseArgs(process.argv.slice(2));
    const currentReports = args.currentPaths.map(readJson);
    const result = compareReportSet(
      readJson(args.baselinePath),
      currentReports,
      {
        thresholds: args.thresholds,
        minimumDeltas: args.minimumDeltas,
      },
    );

    if (args.aggregatePath) {
      mkdirSync(dirname(args.aggregatePath), { recursive: true });
      writeFileSync(
        args.aggregatePath,
        `${JSON.stringify(result.aggregate, null, 2)}\n`,
      );
    }

    const output = formatComparisonResult(result, args.mode);
    console.log(output);

    if (result.regressions.length > 0) {
      const annotation = args.mode === "blocking" ? "error" : "warning";
      console.log(
        `::${annotation}::${result.regressions.length} benchmark regression(s) detected`,
      );
      if (args.mode === "blocking") {
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
