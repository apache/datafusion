#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// Test the release version labeler with stubbed GitHub APIs. No API calls are made.
const assert = require("node:assert/strict");
const { test } = require("node:test");
const labeler = require("./release_version_labeler.js");

const pull = (
  number,
  { base = "main", merged_at = "2026-01-01T00:00:00Z" } = {}
) => ({
  number,
  merged_at,
  base: { ref: base },
});

// `lookups` maps a commit to the pull requests returned on each attempt (the
// last response repeats once the attempts run out), or to an error to throw.
// `failLabels` lists the pull requests that fail to get the label. `label` is
// "missing" when the release label does not exist, or "raced" when another run
// creates it between the lookup and the creation.
async function run({ lookups, commits, failLabels = [], label = "exists" }) {
  const result = {
    listed: [],
    labeled: [],
    delays: [],
    logs: [],
    inFlight: 0,
    maxInFlight: 0,
    createdLabels: 0,
  };
  const attempts = {};
  const github = {
    rest: {
      issues: {
        getLabel: async () => {
          if (label !== "exists") {
            throw Object.assign(new Error("Not Found"), { status: 404 });
          }
        },
        createLabel: async () => {
          if (label === "raced") {
            throw Object.assign(new Error("Validation Failed"), {
              status: 422,
            });
          }
          result.createdLabels++;
        },
        addLabels: async ({ issue_number, labels }) => {
          if (failLabels.includes(issue_number)) {
            throw new Error(`cannot label ${issue_number}`);
          }
          result.labeled.push([issue_number, labels]);
        },
      },
      repos: {
        listPullRequestsAssociatedWithCommit: async ({ commit_sha }) => {
          result.listed.push(commit_sha);
          result.maxInFlight = Math.max(result.maxInFlight, ++result.inFlight);
          // Yield like a real request, so lookups that overlap are counted.
          await Promise.resolve();
          result.inFlight--;
          const responses = lookups[commit_sha];
          if (responses instanceof Error) {
            throw responses;
          }
          attempts[commit_sha] = (attempts[commit_sha] || 0) + 1;
          return {
            data: responses[
              Math.min(attempts[commit_sha], responses.length) - 1
            ],
          };
        },
      },
    },
  };
  const context = {
    repo: { owner: "apache", repo: "datafusion" },
    sha: "head",
    payload: commits ? { commits: commits.map((id) => ({ id })) } : {},
  };
  const [realSetTimeout, realLog] = [globalThis.setTimeout, console.log];
  globalThis.setTimeout = (callback, ms) => {
    result.delays.push(ms);
    callback();
  };
  console.log = (message) => result.logs.push(message);
  let error;
  try {
    await labeler({ github, context });
  } catch (thrown) {
    error = thrown;
  } finally {
    globalThis.setTimeout = realSetTimeout;
    console.log = realLog;
  }
  return { result, error };
}

async function execute(args) {
  const { result, error } = await run(args);
  if (error) {
    throw error;
  }
  return result;
}

test("labels the merged pull request without waiting", async () => {
  const result = await execute({ commits: ["a"], lookups: { a: [[pull(7)]] } });
  assert.equal(result.labeled.length, 1);
  assert.equal(result.labeled[0][0], 7);
  assert.match(result.labeled[0][1][0], /^v\d+\.0\.0$/);
  assert.deepEqual(result.delays, []);
});

test("retries until a merged pull request to main appears", async () => {
  const result = await execute({
    commits: ["a"],
    lookups: {
      a: [
        [],
        [pull(7, { merged_at: null }), pull(8, { base: "branch-55" })],
        [pull(9)],
      ],
    },
  });
  assert.deepEqual(result.listed, ["a", "a", "a"]);
  assert.deepEqual(result.delays, [5000, 5000]);
  assert.deepEqual(
    result.labeled.map(([number]) => number),
    [9]
  );
});

test("gives up without failing and names every commit it searched", async () => {
  const result = await execute({
    commits: ["a", "b"],
    lookups: { a: [[]], b: [[]] },
  });
  assert.equal(result.listed.length, 6);
  assert.deepEqual(result.labeled, []);
  assert.deepEqual(result.logs, ["No merged pull request found for a, b"]);
});

test("labels every pull request in the push once", async () => {
  const result = await execute({
    commits: ["a", "b"],
    lookups: { a: [[pull(1)]], b: [[pull(1), pull(2)]] },
  });
  assert.deepEqual(
    result.labeled.map(([number]) => number),
    [1, 2]
  );
});

test("looks the commits up in parallel and retries only the one that lags", async () => {
  const result = await execute({
    commits: ["a", "b"],
    lookups: { a: [[pull(1)]], b: [[], [pull(2)]] },
  });
  assert.equal(result.maxInFlight, 2);
  assert.equal(result.listed.filter((sha) => sha === "a").length, 1);
  assert.equal(result.listed.filter((sha) => sha === "b").length, 2);
  assert.deepEqual(
    result.labeled.map(([number]) => number),
    [1, 2]
  );
});

test("looks up the pushed commit when the payload has no commits", async () => {
  const result = await execute({ lookups: { head: [[pull(3)]] } });
  assert.deepEqual(result.listed, ["head"]);
  assert.deepEqual(
    result.labeled.map(([number]) => number),
    [3]
  );
});

test("looks up at most ten commits at a time", async () => {
  const commits = Array.from({ length: 25 }, (_, i) => `c${i}`);
  const lookups = Object.fromEntries(
    commits.map((sha, i) => [sha, [[pull(i + 1)]]])
  );
  const result = await execute({ commits, lookups });
  assert.equal(result.maxInFlight, 10);
  assert.equal(result.labeled.length, 25);
});

test("labels the other pull requests when a lookup fails, then fails the run", async () => {
  const failure = new Error("rate limited");
  const { result, error } = await run({
    commits: ["a", "b", "c"],
    lookups: { a: [[pull(1)]], b: failure, c: [[pull(3)]] },
  });
  assert.equal(error, failure);
  assert.deepEqual(
    result.labeled.map(([number]) => number),
    [1, 3]
  );
});

test("labels the other pull requests when one label fails, then fails the run", async () => {
  const { result, error } = await run({
    commits: ["a", "b"],
    lookups: { a: [[pull(1)]], b: [[pull(2)]] },
    failLabels: [1],
  });
  assert.match(error.message, /cannot label 1/);
  assert.deepEqual(
    result.labeled.map(([number]) => number),
    [2]
  );
});

test("logs every failed request with its commit or pull request", async () => {
  const { result, error } = await run({
    commits: ["a", "b"],
    lookups: { a: new Error("rate limited"), b: [[pull(2)]] },
    failLabels: [2],
  });
  assert.equal(error.message, "rate limited");
  assert.deepEqual(result.logs, [
    "Looking up a failed: Error: rate limited",
    "Labeling #2 failed: Error: cannot label 2",
    "2 request(s) failed, see above.",
  ]);
});

test("creates the release label when it is missing", async () => {
  const result = await execute({
    commits: ["a"],
    lookups: { a: [[pull(1)]] },
    label: "missing",
  });
  assert.equal(result.createdLabels, 1);
  assert.deepEqual(
    result.labeled.map(([number]) => number),
    [1]
  );
});

test("labels when another run creates the release label first", async () => {
  const result = await execute({
    commits: ["a"],
    lookups: { a: [[pull(1)]] },
    label: "raced",
  });
  assert.deepEqual(
    result.labeled.map(([number]) => number),
    [1]
  );
});
