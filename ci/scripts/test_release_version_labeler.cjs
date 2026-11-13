#!/usr/bin/env node
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// Test the release version labeler with stubbed GitHub APIs. No API calls are made.
const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const path = require("node:path");
const { test } = require("node:test");
const labeler = require("./release_version_labeler.js");

// The script, and the pattern that dev/update_datafusion_versions.py rewrites.
const source = readFileSync(
  path.join(__dirname, "release_version_labeler.js"),
  "utf8"
);
const VERSION_SCRIPT_PATTERN = /const target_version = '.*?'/g;
const [current] = source.match(VERSION_SCRIPT_PATTERN);
const version = current.slice("const target_version = '".length, -1);
// The script at another release version, as the version script writes it.
const scriptAt = (other) =>
  source.replace(VERSION_SCRIPT_PATTERN, `const target_version = '${other}'`);

const DAY = 24 * 60 * 60 * 1000;
const daysAgo = (days) => new Date(Date.now() - days * DAY).toISOString();
const pull = (
  number,
  { merged = 1, updated = merged, labels = [], commit = "head" } = {}
) => ({
  number,
  merged_at: merged === null ? null : daysAgo(merged),
  updated_at: daysAgo(updated),
  labels: labels.map((name) => ({ name })),
  merge_commit_sha: commit,
});

// `pages` are the pages of recently updated closed pull requests. `scripts`
// maps a merge commit to the script at it, or to an error to throw. `label` is
// "missing" when the release label does not exist, or "raced" when another run
// creates it between the lookup and the creation.
async function run({ pages, scripts = {}, label = "exists", failLabels = [] }) {
  const result = {
    labeled: [],
    read: [],
    pagesRead: 0,
    createdLabels: [],
    logs: [],
  };
  const github = {
    rest: {
      pulls: { list: () => {} },
      repos: {
        getContent: async ({ owner, repo, path: file, ref }) => {
          assert.deepEqual(
            [owner, repo, file],
            ["apache", "datafusion", "ci/scripts/release_version_labeler.js"]
          );
          result.read.push(ref);
          const script = scripts[ref];
          if (script instanceof Error) {
            throw script;
          }
          // The API wraps the content in lines of 60 characters.
          const content = Buffer.from(script)
            .toString("base64")
            .replace(/.{60}/g, "$&\n");
          return { data: { content, encoding: "base64" } };
        },
      },
      issues: {
        getLabel: async () => {
          if (label !== "exists") {
            throw Object.assign(new Error("Not Found"), { status: 404 });
          }
        },
        createLabel: async ({ name }) => {
          if (label === "raced") {
            throw Object.assign(new Error("Validation Failed"), {
              status: 422,
            });
          }
          result.createdLabels.push(name);
        },
        addLabels: async ({ issue_number, labels }) => {
          if (failLabels.includes(issue_number)) {
            throw new Error(`cannot label ${issue_number}`);
          }
          result.labeled.push([issue_number, ...labels]);
        },
      },
    },
    paginate: {
      iterator: async function* (method, params) {
        assert.equal(method, github.rest.pulls.list);
        assert.deepEqual(params, {
          owner: "apache",
          repo: "datafusion",
          state: "closed",
          base: "main",
          sort: "updated",
          direction: "desc",
          per_page: 100,
        });
        for (const data of pages) {
          result.pagesRead++;
          yield { data };
        }
      },
    },
  };
  const context = {
    repo: { owner: "apache", repo: "datafusion" },
    sha: "head",
  };
  const realLog = console.log;
  console.log = (message) => result.logs.push(message);
  let error;
  try {
    await labeler({ github, context });
  } catch (thrown) {
    error = thrown;
  } finally {
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

test("the version script rewrites only the target version", () => {
  assert.equal(source.match(VERSION_SCRIPT_PATTERN).length, 1);
});

test("labels a pull request merged at the pushed commit without reading the script", async () => {
  const result = await execute({ pages: [[pull(1)]] });
  assert.deepEqual(result.labeled, [[1, version]]);
  assert.deepEqual(result.read, []);
});

test("labels a pull request with the version the script names at its merge commit", async () => {
  const result = await execute({
    pages: [[pull(2, { commit: "before-bump" })]],
    scripts: { "before-bump": scriptAt("v1.0.0") },
  });
  assert.deepEqual(result.labeled, [[2, "v1.0.0"]]);
  assert.deepEqual(result.read, ["before-bump"]);
});

test("labels only merged pull requests in the window that have no release label", async () => {
  const result = await execute({
    pages: [
      [
        pull(1, { merged: null, updated: 1 }),
        pull(2, { labels: [version] }),
        pull(3, { labels: ["development-process"] }),
        pull(4, { merged: 8, updated: 1 }),
      ],
    ],
  });
  assert.deepEqual(result.labeled, [[3, version]]);
});

test("stops listing at the first pull request updated before the window", async () => {
  const result = await execute({
    pages: [[pull(1), pull(2, { merged: 8 })], [pull(3)]],
  });
  assert.deepEqual(result.labeled, [[1, version]]);
  assert.equal(result.pagesRead, 1);
});

test("logs and succeeds when no pull request needs a release label", async () => {
  const result = await execute({ pages: [[pull(1, { labels: [version] })]] });
  assert.deepEqual(result.labeled, []);
  assert.deepEqual(result.logs, [
    "No pull request merged in the last 7 days needs a release label",
  ]);
});

test("creates a missing release label once", async () => {
  const result = await execute({
    pages: [[pull(1), pull(2)]],
    label: "missing",
  });
  assert.deepEqual(result.createdLabels, [version]);
  assert.deepEqual(result.labeled, [
    [1, version],
    [2, version],
  ]);
});

test("labels when another run creates the release label first", async () => {
  const result = await execute({ pages: [[pull(1)]], label: "raced" });
  assert.deepEqual(result.labeled, [[1, version]]);
});

test("logs a failure with its pull request, labels the others, then fails", async () => {
  const failure = new Error("Not Found");
  const { result, error } = await run({
    pages: [[pull(1), pull(2, { commit: "gone" }), pull(3)]],
    scripts: { gone: failure },
    failLabels: [3],
  });
  assert.equal(error, failure);
  assert.deepEqual(result.labeled, [[1, version]]);
  assert.deepEqual(result.logs, [
    `Labeled #1 with ${version}`,
    "Labeling #2 failed: Error: Not Found",
    "Labeling #3 failed: Error: cannot label 3",
    "2 pull request(s) failed, see above.",
  ]);
});
