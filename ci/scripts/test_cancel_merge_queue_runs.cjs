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

const assert = require("node:assert/strict");
const { test } = require("node:test");
const { run } = require("./cancel_merge_queue_runs.cjs");

const sha = "a".repeat(40);
const otherSha = "b".repeat(40);
const branch = `gh-readonly-queue/main/pr-123-${"c".repeat(40)}`;
const fullName = "apache/datafusion";
function workflow(id = 1, overrides = {}) {
  return {
    id,
    event: "merge_group",
    status: "in_progress",
    path: ".github/workflows/rust.yml",
    repository: { full_name: fullName },
    head_repository: { full_name: fullName },
    head_branch: branch,
    head_sha: sha,
    run_attempt: 1,
    ...overrides,
  };
}
const apiError = (status) =>
  Object.assign(new Error(`HTTP ${status}`), { status });

async function execute({
  runs = [workflow()],
  refs = {},
  current = {},
  cancelErrors = {},
  dryRun = false,
  omitDryRun = false,
  owner = "apache",
  listError,
  listResponse,
  comparisons = {},
  eventName = "schedule",
} = {}) {
  const calls = [];
  const cancellations = [];
  const failures = [];
  const warnings = [];
  const logs = [];
  const refReads = new Map();
  const runReads = new Map();
  const actions = {
    listWorkflowRunsForRepo: async (params) => {
      assert.equal(params.event, "merge_group");
      assert.equal(params.per_page, 100);
      assert.equal(`${params.owner}/${params.repo}`, fullName);
      calls.push(`list:${params.status}`);
      if (params.status === listError) throw apiError(503);
      if (listResponse) return { data: await listResponse(params) };
      assert.equal(params.created, undefined);
      const filtered = runs.filter((r) => r.status === params.status);
      return {
        data: {
          total_count: filtered.length,
          workflow_runs: filtered.slice(
            (params.page - 1) * 100,
            params.page * 100
          ),
        },
      };
    },
    getWorkflowRun: async ({ run_id }) => {
      calls.push(`get:${run_id}`);
      const index = runReads.get(run_id) ?? 0;
      runReads.set(run_id, index + 1);
      let value = current[run_id] ?? runs.find((r) => r.id === run_id);
      if (Array.isArray(value))
        value = value[Math.min(index, value.length - 1)];
      if (value instanceof Error) throw value;
      return { data: value };
    },
    cancelWorkflowRun: async ({ owner, repo, run_id }) => {
      assert.equal(`${owner}/${repo}`, fullName);
      calls.push(`cancel:${run_id}`);
      if (cancelErrors[run_id]) throw cancelErrors[run_id];
      cancellations.push(run_id);
    },
  };
  const github = {
    rest: {
      actions,
      repos: {
        compareCommitsWithBasehead: async ({ basehead, owner, repo }) => {
          assert.equal(`${owner}/${repo}`, fullName);
          calls.push(`compare:${basehead}`);
          const response = comparisons[basehead] ?? "diverged";
          if (response instanceof Error) throw response;
          return { data: { status: response } };
        },
      },
      git: {
        getRef: async ({ ref, owner, repo }) => {
          assert.equal(`${owner}/${repo}`, fullName);
          calls.push(`ref:${ref}`);
          const index = refReads.get(ref) ?? 0;
          refReads.set(ref, index + 1);
          let value = refs[ref];
          if (Array.isArray(value))
            value = value[Math.min(index, value.length - 1)];
          if (value instanceof Error) throw value;
          if (value === undefined || value === null) throw apiError(404);
          return {
            data:
              typeof value === "string"
                ? { object: { type: "commit", sha: value } }
                : value,
          };
        },
      },
    },
  };
  const results = await run({
    github,
    context: { repo: { owner, repo: "datafusion" }, runId: 999, eventName },
    core: {
      info: (s) => logs.push(s),
      warning: (s) => warnings.push(s),
      setFailed: (s) => failures.push(s),
    },
    ...(omitDryRun ? {} : { dryRun }),
  });
  return { results, cancellations, calls, failures, warnings, logs };
}

test("keeps every workflow of an active group, including failed-check siblings", async () => {
  const result = await execute({
    runs: [
      workflow(1),
      workflow(2, { path: ".github/workflows/extended.yml" }),
    ],
    refs: { [`heads/${branch}`]: sha },
  });
  assert.deepEqual(result.cancellations, []);
  assert.equal(result.calls.filter((c) => c.startsWith("ref:")).length, 1);
});

test("cancels all workflows of a removed group after collecting every active status", async () => {
  const statuses = ["queued", "in_progress", "waiting", "pending", "requested"];
  const result = await execute({
    runs: statuses.map((status, i) => workflow(i + 1, { status })),
  });
  assert.deepEqual(result.cancellations.sort(), [1, 2, 3, 4, 5]);
  assert.deepEqual(
    result.calls.slice(0, 5),
    statuses.map((s) => `list:${s}`)
  );
  assert.ok(result.results.every((r) => r.reason === "queue ref removed"));
});

test("cancels an old SHA when its queue ref points at a replacement", async () => {
  const result = await execute({
    runs: [workflow(1), workflow(2, { head_sha: otherSha })],
    refs: { [`heads/${branch}`]: otherSha },
  });
  assert.deepEqual(result.cancellations, [1]);
  assert.equal(result.results[0].reason, "queue ref replaced");
});

test("new base ref for one PR does not cancel its replacement or another PR", async () => {
  const replacement = `gh-readonly-queue/main/pr-123-${"d".repeat(40)}`;
  const unrelated = `gh-readonly-queue/main/pr-456-${"e".repeat(40)}`;
  const result = await execute({
    runs: [
      workflow(1),
      workflow(2, { head_branch: replacement }),
      workflow(3, { head_branch: unrelated }),
    ],
    refs: { [`heads/${replacement}`]: sha, [`heads/${unrelated}`]: sha },
  });
  assert.deepEqual(result.cancellations, [1]);
});

test("checks the full queue ref, including a base branch containing slashes", async () => {
  const release = `gh-readonly-queue/release/56/pr-123-${"c".repeat(40)}`;
  const result = await execute({
    runs: [workflow(1), workflow(2, { head_branch: release })],
    refs: { [`heads/${release}`]: sha },
  });
  assert.deepEqual(result.cancellations, [1]);
});

test("ignores non-queue events, foreign repos, malformed refs, and cleanup runs", async () => {
  const overrides = [
    { event: "push" },
    { event: "pull_request" },
    { event: "workflow_dispatch" },
    { head_repository: { full_name: "contributor/datafusion" } },
    { repository: { full_name: "contributor/datafusion" } },
    { head_repository: null },
    { head_branch: "main" },
    { head_branch: "gh-readonly-queue/main/pr-123-invalid" },
    { head_sha: "invalid" },
    { path: ".github/workflows/merge_queue_cleanup.yml" },
    { path: ".github/workflows/merge_queue_cleanup.yml@refs/heads/main" },
    { id: 999 },
    { status: "completed" },
  ];
  const result = await execute({
    runs: overrides.map((value, i) => workflow(i + 1, value)),
  });
  assert.deepEqual(result.cancellations, []);
  assert.equal(result.calls.length, 5);
});

test("does nothing on a fork", async () => {
  const result = await execute({ owner: "contributor" });
  assert.deepEqual(result.calls, []);
});

test("dry run performs the same checks without mutations", async () => {
  const result = await execute({ dryRun: true });
  assert.deepEqual(result.cancellations, []);
  assert.equal(result.results[0].id, 1);
  assert.equal(result.results[0].sha, sha);
  assert.equal(result.results[0].attempt, 1);
  assert.equal(result.results[0].reason, "queue ref removed");
  assert.ok(Number.isFinite(Date.parse(result.results[0].observed_at)));
  assert.match(result.logs[0], /Would cancel/);
});

test("defaults to a dry run when the caller does not specify a mode", async () => {
  const result = await execute({ omitDryRun: true });
  assert.deepEqual(result.cancellations, []);
  assert.equal(result.results.length, 1);
  assert.match(result.logs[0], /Would cancel/);
});

for (const status of [401, 403, 429, 500]) {
  test(`a ${status} ref lookup cannot be mistaken for a removed ref`, async () => {
    const result = await execute({
      refs: { [`heads/${branch}`]: apiError(status) },
    });
    assert.deepEqual(result.cancellations, []);
    assert.equal(result.failures.length, 1);
  });
}

test("malformed ref responses cannot cancel a run", async () => {
  for (const response of [
    {},
    { object: { type: "tag", sha: otherSha } },
    { object: { type: "commit", sha: "" } },
  ]) {
    const result = await execute({ refs: { [`heads/${branch}`]: response } });
    assert.deepEqual(result.cancellations, []);
    assert.equal(result.failures.length, 1);
  }
});

test("rechecks refs and keeps a group that is active again", async () => {
  const result = await execute({ refs: { [`heads/${branch}`]: [null, sha] } });
  assert.deepEqual(result.cancellations, []);
});

test("a failed second ref read cannot cancel a run", async () => {
  const result = await execute({
    refs: { [`heads/${branch}`]: [null, apiError(503)] },
  });
  assert.deepEqual(result.cancellations, []);
  assert.equal(result.failures.length, 1);
});

test("does not cancel completed runs, newer attempts, or changed run identities", async () => {
  for (const update of [
    { status: "completed" },
    { run_attempt: 2 },
    { head_sha: otherSha },
    { head_branch: `gh-readonly-queue/main/pr-456-${"c".repeat(40)}` },
    { event: "push" },
  ]) {
    const result = await execute({ current: { 1: workflow(1, update) } });
    assert.deepEqual(result.cancellations, []);
  }
});

test("409 on cancellation is a harmless completion race", async () => {
  const result = await execute({
    current: { 1: [workflow(), workflow(1, { status: "completed" })] },
    cancelErrors: { 1: apiError(409) },
  });
  assert.deepEqual(result.failures, []);
  assert.deepEqual(result.results, []);
});

test("409 for an unchanged active run reports a failed cancellation", async () => {
  const result = await execute({ cancelErrors: { 1: apiError(409) } });
  assert.equal(result.failures.length, 1);
  assert.match(result.warnings[0], /HTTP 409/);
});

test("409 after a new rerun starts leaves that attempt alone", async () => {
  const result = await execute({
    current: { 1: [workflow(), workflow(1, { run_attempt: 2 })] },
    cancelErrors: { 1: apiError(409) },
  });
  assert.deepEqual(result.failures, []);
  assert.deepEqual(result.results, []);
});

test("409 on a read is still an error", async () => {
  const result = await execute({ current: { 1: apiError(409) } });
  assert.deepEqual(result.cancellations, []);
  assert.equal(result.failures.length, 1);
});

test("one cancellation failure does not stop cleanup of other runs", async () => {
  const result = await execute({
    runs: [workflow(1), workflow(2)],
    cancelErrors: { 1: apiError(403) },
  });
  assert.deepEqual(result.cancellations, [2]);
  assert.equal(result.failures.length, 1);
});

test("one failed group lookup does not stop cleanup of another group", async () => {
  const result = await execute({
    runs: [
      workflow(1),
      workflow(2, {
        head_branch: `gh-readonly-queue/main/pr-456-${"c".repeat(40)}`,
      }),
    ],
    refs: { [`heads/${branch}`]: apiError(503) },
  });
  assert.deepEqual(result.cancellations, [2]);
  assert.equal(result.failures.length, 1);
});

test("a listing failure aborts before any cancellation", async () => {
  const result = await execute({ listError: "requested" });
  assert.deepEqual(result.cancellations, []);
  assert.equal(result.failures.length, 1);
  assert.match(result.warnings[0], /HTTP 503/);
});

test("uses all paginated results and deduplicates repeated run IDs", async () => {
  const runs = Array.from({ length: 205 }, (_, i) => workflow(i + 1));
  runs.push(runs[0]);
  const result = await execute({ runs });
  assert.equal(result.cancellations.length, 205);
  assert.equal(new Set(result.cancellations).size, 205);
});

test("keeps merged commits even when their queue ref has disappeared or changed", async () => {
  for (const status of ["identical", "behind"]) {
    for (const ref of [null, otherSha]) {
      const result = await execute({
        comparisons: { [`main...${sha}`]: status },
        refs: { [`heads/${branch}`]: ref },
      });
      assert.deepEqual(result.cancellations, []);
      assert.deepEqual(result.failures, []);
    }
  }
});

test("compares against the actual target branch, including slashes", async () => {
  const result = await execute({
    runs: [
      workflow(1, {
        head_branch: `gh-readonly-queue/release/56/pr-123-${sha}`,
      }),
    ],
    comparisons: { [`release/56...${sha}`]: "behind" },
  });
  assert.deepEqual(result.cancellations, []);
  assert.ok(result.calls.includes(`compare:release/56...${sha}`));
});

test("only ahead or diverged comparisons permit cancellation", async () => {
  for (const status of ["ahead", "diverged"]) {
    const result = await execute({
      comparisons: { [`main...${sha}`]: status },
    });
    assert.deepEqual(result.cancellations, [1]);
  }
  for (const status of [
    "unknown",
    "",
    apiError(404),
    apiError(403),
    apiError(503),
  ]) {
    const result = await execute({
      comparisons: { [`main...${sha}`]: status },
    });
    assert.deepEqual(result.cancellations, []);
    assert.equal(result.failures.length, 1);
  }
});

test("reruns are exempt across successive scans, not just on attempt changes", async () => {
  for (let scan = 0; scan < 2; scan++) {
    const result = await execute({
      runs: [workflow(1, { run_attempt: 2 }), workflow(2)],
    });
    assert.deepEqual(result.cancellations, [2]);
    assert.ok(!result.calls.includes("get:1"));
  }
});

test("merge-group maintenance errors warn without failing the commit", async () => {
  for (const options of [
    { listError: "requested" },
    { refs: { [`heads/${branch}`]: apiError(403) } },
    { comparisons: { [`main...${sha}`]: apiError(503) } },
    { cancelErrors: { 1: apiError(409) } },
  ]) {
    const result = await execute({ eventName: "merge_group", ...options });
    assert.deepEqual(result.failures, []);
    assert.ok(result.warnings.length > 0);
    assert.deepEqual(result.cancellations, []);
  }
});

test("manual cleanup failures remain visible", async () => {
  const result = await execute({
    eventName: "workflow_dispatch",
    cancelErrors: { 1: apiError(409) },
  });
  assert.equal(result.failures.length, 1);
});

test("an unresolved 409 is retried by later scans and is never reported as cancelled", async () => {
  const first = await execute({ cancelErrors: { 1: apiError(409) } });
  assert.deepEqual(first.results, []);
  assert.equal(first.failures.length, 1);
  const next = await execute();
  assert.deepEqual(next.cancellations, [1]);
});

test("a capped listing aborts without cancelling", async () => {
  const result = await execute({
    runs: Array.from({ length: 1000 }, (_, i) => workflow(i + 1)),
  });
  assert.deepEqual(result.cancellations, []);
  assert.deepEqual(
    result.calls.filter((c) => !c.startsWith("list:")),
    []
  );
  assert.equal(result.failures.length, 1);
  assert.match(result.warnings[0], /exceed the listing cap/);
});

test("workflows of one group share the target-branch comparison", async () => {
  const result = await execute({
    runs: [
      workflow(1),
      workflow(2, { path: ".github/workflows/extended.yml" }),
    ],
  });
  assert.deepEqual(result.cancellations, [1, 2]);
  assert.equal(result.calls.filter((c) => c.startsWith("compare:")).length, 1);
});

test("malformed listing metadata aborts without cancelling", async () => {
  const result = await execute({
    listResponse: () => ({ workflow_runs: [workflow()] }),
  });
  assert.deepEqual(result.cancellations, []);
  assert.equal(result.failures.length, 1);
});
