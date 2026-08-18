#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const FULL_COMMIT_SHA = /^[0-9a-f]{40}$/;
const USES_KEY = /^\s*(?:-\s*)?uses:\s*(.*)$/;

function parseUsesValue(rawValue) {
  const quoted = rawValue.match(/^(["'])([^"']+)\1(?:\s+#.*)?$/);
  if (quoted) {
    return quoted[2];
  }

  const unquoted = rawValue.match(/^([^\s#"']+)(?:\s+#.*)?$/);
  return unquoted?.[1];
}

export function inspectWorkflowSource(source, file = "workflow.yml") {
  const violations = [];
  let remoteActionCount = 0;

  for (const [index, line] of source.split(/\r?\n/).entries()) {
    const usesMatch = line.match(USES_KEY);
    if (!usesMatch) {
      continue;
    }

    const value = parseUsesValue(usesMatch[1].trim());
    if (!value) {
      violations.push({
        file,
        line: index + 1,
        message: "cannot parse uses reference",
      });
      continue;
    }

    if (value.startsWith("./") || value.startsWith("docker://")) {
      continue;
    }

    remoteActionCount += 1;
    const separator = value.lastIndexOf("@");
    if (separator <= 0 || separator === value.length - 1) {
      violations.push({
        file,
        line: index + 1,
        message: `remote action is missing a ref: ${value}`,
      });
      continue;
    }

    const action = value.slice(0, separator);
    const ref = value.slice(separator + 1);
    if (!action.includes("/") || !FULL_COMMIT_SHA.test(ref)) {
      violations.push({
        file,
        line: index + 1,
        message: `remote action must use a full commit SHA: ${value}`,
      });
    }
  }

  return { remoteActionCount, violations };
}

function main() {
  const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
  const workflowDirectory = path.join(repositoryRoot, ".github", "workflows");
  const workflowFiles = fs
    .readdirSync(workflowDirectory)
    .filter((file) => file.endsWith(".yml") || file.endsWith(".yaml"))
    .sort();

  let remoteActionCount = 0;
  const violations = [];

  for (const file of workflowFiles) {
    const source = fs.readFileSync(path.join(workflowDirectory, file), "utf8");
    const result = inspectWorkflowSource(source, file);
    remoteActionCount += result.remoteActionCount;
    violations.push(...result.violations);
  }

  if (violations.length > 0) {
    for (const violation of violations) {
      console.error(`${violation.file}:${violation.line}: ${violation.message}`);
    }
    process.exitCode = 1;
    return;
  }

  console.log(
    `Validated ${remoteActionCount} remote GitHub Action references: all are SHA-pinned.`,
  );
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main();
}
