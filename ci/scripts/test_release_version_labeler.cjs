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

// `lookups` maps a commit to the pull requests returned on each attempt. The
// last response repeats once the attempts run out.
async function execute({ lookups, commits }) {
  const result = {
    listed: [],
    labeled: [],
    delays: [],
    logs: [],
    inFlight: 0,
    maxInFlight: 0,
  };
  const attempts = {};
  const github = {
    rest: {
      issues: {
        getLabel: async () => {},
        addLabels: async ({ issue_number, labels }) =>
          result.labeled.push([issue_number, labels]),
      },
      repos: {
        listPullRequestsAssociatedWithCommit: async ({ commit_sha }) => {
          result.listed.push(commit_sha);
          result.maxInFlight = Math.max(result.maxInFlight, ++result.inFlight);
          // Yield like a real request, so lookups that overlap are counted.
          await Promise.resolve();
          result.inFlight--;
          const responses = lookups[commit_sha];
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
  try {
    await labeler({ github, context });
  } finally {
    globalThis.setTimeout = realSetTimeout;
    console.log = realLog;
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
