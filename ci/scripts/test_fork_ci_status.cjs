#!/usr/bin/env node
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Execute the workflow's script with stubbed GitHub APIs. No API calls are made.
const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");

const script = execFileSync(
  "python3",
  [
    "-c",
    'import sys, yaml; print(yaml.safe_load(sys.stdin)["jobs"]["bridge"]["steps"][0]["with"]["script"])',
  ],
  {
    input: readFileSync(
      path.join(__dirname, "../../.github/workflows/fork_ci_status.yml")
    ),
    encoding: "utf8",
  }
);
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const bridge = new AsyncFunction("github", "context", "core", script);
const sha = "a".repeat(40);
const pr = {
  number: 1,
  updated_at: new Date().toISOString(),
  head: {
    sha,
    repo: { owner: { login: "contributor" }, name: "datafusion", fork: true },
  },
  base: { ref: "main" },
};
const run = (id, event = "push", conclusion = "success", run_attempt = 1) => ({
  id,
  event,
  conclusion,
  run_attempt,
  status: "completed",
  html_url: `https://github.com/contributor/datafusion/actions/runs/${id}`,
});
const checkFor = (run) => ({
  id: 10,
  status: run.status,
  conclusion: run.conclusion,
  output: {
    text: JSON.stringify({
      owner: "contributor",
      repo: "datafusion",
      head_sha: sha,
      run_id: run.id,
      run_attempt: run.run_attempt,
    }),
  },
});

async function execute({
  check,
  runs = [],
  recordedRun,
  event = "schedule",
  workflowState = "active",
}) {
  const writes = [];
  const github = {
    rest: {
      checks: {
        listForRef: async () => ({
          data: { check_runs: check ? [check] : [] },
        }),
        create: async (payload) =>
          writes.push({ method: "create", ...payload }),
        update: async (payload) =>
          writes.push({ method: "update", ...payload }),
      },
      actions: {
        listWorkflowRuns: async (params) => {
          assert.equal(params.owner, "contributor");
          assert.equal(params.repo, "datafusion");
          assert.equal(params.workflow_id, "rust.yml");
          assert.equal(params.head_sha, sha);
          return { data: { workflow_runs: runs } };
        },
        getWorkflowRun: async () => {
          if (!recordedRun) throw new Error("Not Found");
          return { data: recordedRun };
        },
        getWorkflow: async () => ({ data: { state: workflowState } }),
      },
      pulls: { list: async () => ({ data: [pr], headers: {} }) },
    },
    paginate: {
      iterator: async function* () {
        yield { data: [pr] };
      },
    },
  };
  await bridge(
    github,
    {
      repo: { owner: "apache", repo: "datafusion" },
      eventName: event,
      payload: { pull_request: pr },
    },
    { info: () => {} }
  );
  return writes;
}

function assertRun(write, method, expected) {
  assert.equal(write.method, method);
  assert.equal(write.status, expected.status);
  assert.equal(write.conclusion, expected.conclusion);
  assert.equal(write.details_url, expected.html_url);
  assert.equal(JSON.parse(write.output.text).run_id, expected.id);
  assert.equal(JSON.parse(write.output.text).run_attempt, expected.run_attempt);
}

test("notification selects a newer manual run instead of a push run", async () => {
  const latest = run(2, "workflow_dispatch");
  const writes = await execute({
    event: "pull_request_target",
    runs: [latest, run(1)],
  });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "create", latest);
});

for (const conclusion of ["cancelled", "failure"]) {
  test(`sweep replaces a ${conclusion} push run with a newer manual run`, async () => {
    const previous = run(1, "push", conclusion);
    const latest = run(2, "workflow_dispatch");
    const writes = await execute({
      check: checkFor(previous),
      recordedRun: previous,
      runs: [latest, previous],
    });
    assert.equal(writes.length, 1);
    assertRun(writes[0], "update", latest);
  });
}

test("sweep recovers when the recorded run was deleted", async () => {
  const latest = run(2, "workflow_dispatch");
  const writes = await execute({ check: checkFor(run(1)), runs: [latest] });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "update", latest);
});

test("sweep creates a missing check from the PR head", async () => {
  const latest = run(2);
  const writes = await execute({ runs: [latest] });
  assert.equal(writes.length, 1);
  assert.equal(writes[0].head_sha, sha);
  assertRun(writes[0], "create", latest);
});

for (const [state, conclusion] of [
  ["active", "skipped"],
  ["disabled_fork", "action_required"],
]) {
  test(`sweep creates a missing check when the workflow is ${state} and has no run`, async () => {
    const writes = await execute({ workflowState: state });
    assert.equal(writes.length, 1);
    assert.equal(writes[0].method, "create");
    assert.equal(writes[0].head_sha, sha);
    assert.equal(writes[0].conclusion, conclusion);
    assert.equal(JSON.parse(writes[0].output.text).run_id, null);
  });
}

test("sweep picks up a rerun of the same workflow run", async () => {
  const previous = run(1, "push", "failure");
  const latest = run(1, "push", "success", 2);
  const writes = await execute({
    check: checkFor(previous),
    recordedRun: latest,
    runs: [latest],
  });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "update", latest);
});

test("sweep does not rewrite an unchanged check", async () => {
  const latest = run(1);
  assert.deepEqual(
    await execute({
      check: checkFor(latest),
      recordedRun: latest,
      runs: [latest],
    }),
    []
  );
});
