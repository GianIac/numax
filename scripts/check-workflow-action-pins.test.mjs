import assert from "node:assert/strict";
import test from "node:test";

import { inspectWorkflowSource } from "./check-workflow-action-pins.mjs";

const SHA = "d23441a48e516b6c34aea4fa41551a30e30af803";

test("accepts full commit SHAs and ignores local or Docker actions", () => {
  const source = `
steps:
  - uses: actions/checkout@${SHA} # v6.1.0
  - uses: "owner/action/path@${SHA}"
  - uses: ./local-action
  - uses: docker://alpine:3.22
`;

  assert.deepEqual(inspectWorkflowSource(source), {
    remoteActionCount: 2,
    violations: [],
  });
});

test("rejects tags, branches, short SHAs, and missing refs", () => {
  const source = `
steps:
  - uses: actions/checkout@v6
  - uses: actions/checkout@main
  - uses: actions/checkout@d23441a
  - uses: actions/checkout
`;

  const result = inspectWorkflowSource(source, "ci.yml");
  assert.equal(result.remoteActionCount, 4);
  assert.deepEqual(
    result.violations.map(({ line, message }) => ({ line, message })),
    [
      {
        line: 3,
        message: "remote action must use a full commit SHA: actions/checkout@v6",
      },
      {
        line: 4,
        message: "remote action must use a full commit SHA: actions/checkout@main",
      },
      {
        line: 5,
        message: "remote action must use a full commit SHA: actions/checkout@d23441a",
      },
      {
        line: 6,
        message: "remote action is missing a ref: actions/checkout",
      },
    ],
  );
});

test("rejects references that cannot be parsed safely", () => {
  const result = inspectWorkflowSource("- uses: 'actions/checkout@v6\n", "ci.yml");

  assert.equal(result.remoteActionCount, 0);
  assert.deepEqual(result.violations, [
    {
      file: "ci.yml",
      line: 1,
      message: "cannot parse uses reference",
    },
  ]);
});
