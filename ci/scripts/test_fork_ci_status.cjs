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

// Test fork CI workflows with stubbed GitHub APIs. No API calls are made.
const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");

function loadWorkflow(name) {
  return JSON.parse(
    execFileSync(
      "python3",
      [
        "-c",
        "import json, sys, yaml; print(json.dumps(yaml.safe_load(sys.stdin)))",
      ],
      {
        input: readFileSync(
          path.join(__dirname, "../../.github/workflows", name)
        ),
        encoding: "utf8",
      }
    )
  );
}
const script =
  loadWorkflow("fork_ci_status.yml").jobs.bridge.steps[0].with.script;
const rustWorkflow = loadWorkflow("rust.yml");
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const bridge = new AsyncFunction("github", "context", "core", "Date", script);
const now = Date.UTC(2026, 0, 1);
class Clock extends Date {
  static now() {
    return now;
  }
}
const sha = "a".repeat(40);
const pr = {
  number: 1,
  updated_at: new Date(now).toISOString(),
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
  recentPrs = [pr],
  slice = [pr],
  calls = [],
}) {
  const writes = [];
  const github = {
    rest: {
      checks: {
        listForRef: async () => {
          calls.push("checks.listForRef");
          return { data: { check_runs: check ? [check] : [] } };
        },
        create: async (payload) =>
          writes.push({ method: "create", ...payload }),
        update: async (payload) =>
          writes.push({ method: "update", ...payload }),
      },
      actions: {
        listWorkflowRuns: async (params) => {
          calls.push("actions.listWorkflowRuns");
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
      pulls: {
        list: async ({ per_page }) => {
          if (per_page === 1) return { data: [pr], headers: {} };
          assert.equal(per_page, 25);
          return { data: slice };
        },
      },
    },
    paginate: {
      iterator: async function* () {
        yield { data: recentPrs };
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
    { info: () => {} },
    Clock
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

for (const conclusion of ["success", "failure", "cancelled"]) {
  test(`recent completed ${conclusion} checks wait for their sweep slice`, async () => {
    const calls = [];
    const previous = run(1, "push", conclusion);
    const writes = await execute({
      check: checkFor(previous),
      runs: [run(2, "workflow_dispatch")],
      slice: [],
      calls,
    });
    assert.deepEqual(writes, []);
    assert.deepEqual(calls, ["checks.listForRef"]);
  });
}

test("the sweep finds a rerun on a recently updated completed PR", async () => {
  const calls = [];
  const latest = run(2, "workflow_dispatch");
  const writes = await execute({
    check: checkFor(run(1)),
    runs: [latest],
    calls,
  });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "update", latest);
  assert.deepEqual(calls, ["checks.listForRef", "actions.listWorkflowRuns"]);
});

test("the sweep still checks completed runs on older PRs", async () => {
  const oldPr = {
    ...pr,
    updated_at: new Date(now - 24 * 3600 * 1000).toISOString(),
  };
  const latest = run(2, "workflow_dispatch");
  const writes = await execute({
    check: checkFor(run(1)),
    runs: [latest],
    recentPrs: [oldPr],
    slice: [oldPr],
  });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "update", latest);
});

for (const status of ["queued", "in_progress"]) {
  test(`recent ${status} runs are polled outside the sweep slice`, async () => {
    const previous = { ...run(1), status, conclusion: null };
    const latest = run(1);
    const writes = await execute({
      check: checkFor(previous),
      runs: [latest],
      slice: [],
    });
    assert.equal(writes.length, 1);
    assertRun(writes[0], "update", latest);
  });
}

test("recent checks with no fork run keep looking outside the sweep slice", async () => {
  const missing = checkFor(run(null, "push", "skipped"));
  const latest = run(2);
  const writes = await execute({ check: missing, runs: [latest], slice: [] });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "update", latest);
});

test("recent missing checks are recovered outside the sweep slice", async () => {
  const latest = run(2);
  const writes = await execute({ runs: [latest], slice: [] });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "create", latest);
});

test("a recent active PR in the sweep slice is only polled once", async () => {
  const calls = [];
  const active = { ...run(1), status: "in_progress", conclusion: null };
  await execute({ check: checkFor(active), runs: [active], calls });
  assert.deepEqual(calls, ["checks.listForRef", "actions.listWorkflowRuns"]);
});

// The concurrency expressions use string lookups and &&/||, whose semantics
// match JavaScript for these non-empty strings and booleans.
function concurrencyGroup(github) {
  return rustWorkflow.concurrency.group.replace(
    /\$\{\{\s*(.*?)\s*\}\}/g,
    (_, expression) => new Function("github", `return (${expression});`)(github)
  );
}
const forkPush = {
  repository: "contributor/datafusion",
  head_ref: "",
  sha: "first",
  ref: "refs/heads/feature",
  workflow: "Rust",
};

test("successive pushes to a fork branch cancel the previous run", () => {
  assert.equal(rustWorkflow.concurrency["cancel-in-progress"], true);
  assert.equal(
    concurrencyGroup(forkPush),
    concurrencyGroup({ ...forkPush, sha: "second" })
  );
});

test("different fork branches have separate concurrency groups", () => {
  assert.notEqual(
    concurrencyGroup(forkPush),
    concurrencyGroup({ ...forkPush, ref: "refs/heads/other" })
  );
});

for (const ref of [
  "refs/heads/main",
  "refs/heads/gh-readonly-queue/main/pr-1",
]) {
  test(`upstream ${ref} runs keep separate groups for each SHA`, () => {
    const upstream = { ...forkPush, repository: "apache/datafusion", ref };
    assert.notEqual(
      concurrencyGroup(upstream),
      concurrencyGroup({ ...upstream, sha: "second" })
    );
  });
}

test("upstream PR updates still share a concurrency group", () => {
  const upstream = {
    ...forkPush,
    repository: "apache/datafusion",
    head_ref: "feature",
    ref: "refs/pull/1/merge",
  };
  assert.equal(
    concurrencyGroup(upstream),
    concurrencyGroup({ ...upstream, sha: "second" })
  );
});
