#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
const script = loadWorkflow("fork_ci_status.yml").jobs.bridge.steps.find(
  (step) => step.with && step.with.script
).with.script;
const rustWorkflow = loadWorkflow("rust.yml");
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
const bridge = new AsyncFunction(
  "github",
  "context",
  "core",
  "Date",
  "require",
  script
);
// actions/github-script resolves relative paths from the checked out workspace.
const workspaceRequire = (file) => require(path.join(__dirname, "../..", file));
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
const workflowRun = {
  head_sha: sha,
  head_branch: "feature",
  head_repository: { owner: { login: "contributor" } },
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
  openPrs = [pr],
  triggeringRun = workflowRun,
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
        list: async ({ per_page, head, base }) => {
          if (head) {
            calls.push("pulls.list");
            assert.equal(head, "contributor:feature");
            return { data: openPrs.filter((p) => p.base.ref === base) };
          }
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
      payload: { workflow_run: triggeringRun },
    },
    { info: () => {} },
    Clock,
    workspaceRequire
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
    event: "workflow_run",
    runs: [latest, run(1)],
  });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "create", latest);
});

test("notification mirrors the pull request to main when a newer one targets a release branch", async () => {
  const release = { ...pr, number: 2, base: { ref: "branch-55" } };
  const latest = run(2, "workflow_dispatch");
  const writes = await execute({
    event: "workflow_run",
    openPrs: [release, pr],
    runs: [latest],
  });
  assert.equal(writes.length, 1);
  assertRun(writes[0], "create", latest);
});

test("notification skips a commit that is no longer a pull request head", async () => {
  const writes = await execute({
    event: "workflow_run",
    openPrs: [{ ...pr, head: { ...pr.head, sha: "b".repeat(40) } }],
    runs: [run(1)],
  });
  assert.deepEqual(writes, []);
});

test("notification skips a run whose fork was deleted", async () => {
  const calls = [];
  const writes = await execute({
    event: "workflow_run",
    triggeringRun: { ...workflowRun, head_repository: null },
    runs: [run(1)],
    calls,
  });
  assert.deepEqual(writes, []);
  assert.deepEqual(calls, []);
});

// The labeler uses the same lookup without a base, to label every match.
const findPullRequests = require("./pull_requests_of_run.js");
const lookup = async ({ openPrs, base, triggeringRun = workflowRun }) => {
  const calls = [];
  const github = {
    rest: {
      pulls: {
        list: async (params) => {
          calls.push(params);
          return { data: openPrs };
        },
      },
    },
  };
  const context = {
    repo: { owner: "apache", repo: "datafusion" },
    payload: { workflow_run: triggeringRun },
  };
  const found = await findPullRequests({ github, context, base });
  return { found: found.map((p) => p.number), calls };
};

test("lookup returns every pull request from the head commit and no stale one", async () => {
  const stale = {
    ...pr,
    number: 3,
    head: { ...pr.head, sha: "b".repeat(40) },
  };
  const release = { ...pr, number: 2, base: { ref: "branch-55" } };
  const { found, calls } = await lookup({ openPrs: [stale, release, pr] });
  assert.deepEqual(found, [2, 1]);
  assert.deepEqual(calls, [
    {
      owner: "apache",
      repo: "datafusion",
      state: "open",
      head: "contributor:feature",
    },
  ]);
});

test("lookup passes the base to the API", async () => {
  const { calls } = await lookup({ openPrs: [pr], base: "main" });
  assert.equal(calls[0].base, "main");
});

test("lookup returns nothing when the fork was deleted", async () => {
  const { found, calls } = await lookup({
    openPrs: [pr],
    triggeringRun: { ...workflowRun, head_repository: null },
  });
  assert.deepEqual(found, []);
  assert.deepEqual(calls, []);
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

// The concurrency expressions use string lookups, format() and &&/||, whose
// semantics match JavaScript for these non-empty strings and booleans. A
// missing or null property is null in a GitHub expression, so read it with ?.
// and print it as an empty string in format().
const format = (template, ...args) =>
  template.replace(/\{(\d+)\}/g, (_, index) => args[index] ?? "");
function concurrencyGroup(github, workflow = rustWorkflow) {
  return workflow.concurrency.group.replace(
    /\$\{\{\s*(.*?)\s*\}\}/g,
    (_, expression) =>
      new Function(
        "github",
        "format",
        `return (${expression.replace(/\./g, "?.")});`
      )(github, format)
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

const forkStatus = loadWorkflow("fork_ci_status.yml");
const signalRun = (head_repository, head_branch = "feature") => ({
  workflow: "Fork CI status",
  event_name: "workflow_run",
  event: { workflow_run: { head_repository, head_branch } },
});
const sweepRun = {
  workflow: "Fork CI status",
  event_name: "schedule",
  event: {},
};

test("signal runs for a fork branch share a group and other branches do not", () => {
  const contributor = { full_name: "contributor/datafusion" };
  const group = (github) => concurrencyGroup(github, forkStatus);
  assert.equal(group(signalRun(contributor)), group(signalRun(contributor)));
  assert.notEqual(
    group(signalRun(contributor)),
    group(signalRun(contributor, "other"))
  );
});

test("a signal run from a deleted fork does not join the group of the sweep", () => {
  const group = (github) => concurrencyGroup(github, forkStatus);
  assert.notEqual(group(signalRun(null)), group(sweepRun));
  assert.equal(
    group(sweepRun),
    group({ ...sweepRun, event_name: "workflow_dispatch" })
  );
});
