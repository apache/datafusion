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

const { setTimeout } = require("node:timers/promises");

const IDLE_DAYS = 14;
const QUERY = `query(
  $owner: String!, $repo: String!, $number: Int!,
  $checksCursor: String, $reviewsCursor: String,
  $loadChecks: Boolean!, $loadReviews: Boolean!
) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      id number state isDraft updatedAt headRefOid mergeable
      mergeQueueEntry { id }
      latestOpinionatedReviews(first: 100, after: $reviewsCursor) @include(if: $loadReviews) {
        nodes { state commit { oid } }
        pageInfo { hasNextPage endCursor }
      }
      commits(last: 1) @include(if: $loadChecks) {
        nodes { commit {
          statusCheckRollup {
            contexts(first: 100, after: $checksCursor) {
              nodes {
                ... on CheckRun { status conclusion }
                ... on StatusContext { state }
              }
              pageInfo { hasNextPage endCursor }
            }
          }
        } }
      }
    }
  }
}`;

const sameVersion = (a, b) =>
  a.headRefOid === b.headRefOid && a.updatedAt === b.updatedAt;

async function loadPullRequest(github, repo, number) {
  const variables = { ...repo, number, loadChecks: true, loadReviews: true };
  let pr;
  const checks = [];
  const reviews = [];
  while (variables.loadChecks || variables.loadReviews) {
    const { repository } = await github.graphql(QUERY, variables);
    const current = repository.pullRequest;
    if (!current || (pr && !sameVersion(pr, current))) return null;
    pr = { ...pr, ...current };
    if (variables.loadChecks) {
      const connection =
        current.commits.nodes[0]?.commit.statusCheckRollup?.contexts;
      checks.push(...(connection?.nodes ?? []));
      variables.loadChecks = connection?.pageInfo.hasNextPage ?? false;
      variables.checksCursor = connection?.pageInfo.endCursor;
    }
    if (variables.loadReviews) {
      const connection = current.latestOpinionatedReviews;
      reviews.push(...connection.nodes);
      variables.loadReviews = connection.pageInfo.hasNextPage;
      variables.reviewsCursor = connection.pageInfo.endCursor;
    }
  }
  return { ...pr, checks, reviews };
}

function reasonsToDraft(pr, cutoff) {
  if (
    !pr ||
    pr.state !== "OPEN" ||
    pr.isDraft ||
    pr.mergeQueueEntry ||
    !(Date.parse(pr.updatedAt) <= cutoff)
  )
    return [];

  const reasons = [];
  if (pr.mergeable === "CONFLICTING") reasons.push("merge conflicts");
  // The rollup reports FAILURE for cancelled checks too.
  if (
    pr.checks.some(
      (check) =>
        (check.status === "COMPLETED" &&
          ["FAILURE", "TIMED_OUT"].includes(check.conclusion)) ||
        ["FAILURE", "ERROR"].includes(check.state)
    )
  )
    reasons.push("failing CI checks");

  if (
    pr.reviews.some(
      (review) =>
        review.state === "CHANGES_REQUESTED" &&
        review.commit?.oid === pr.headRefOid
    )
  )
    reasons.push("changes requested on the latest push");
  return reasons;
}

async function run({ github, context, core, dryRun = true }) {
  const repo = { owner: context.repo.owner, repo: context.repo.repo };
  const cutoff = Date.now() - IDLE_DAYS * 24 * 60 * 60 * 1000;
  // Collect the list before mutations change updated_at and its pagination.
  const prs = await github.paginate(github.rest.pulls.list, {
    ...repo,
    state: "open",
    sort: "updated",
    direction: "asc",
    per_page: 100,
  });
  const candidates = prs.filter(
    (listed) => !listed.draft && Date.parse(listed.updated_at) <= cutoff
  );
  if (!candidates.length) return [];

  // GitHub computes mergeability lazily; warm every candidate before deciding.
  for (const listed of candidates) {
    await loadPullRequest(github, repo, listed.number);
  }
  await setTimeout(30_000);

  const results = [];
  for (const listed of candidates) {
    let pr = await loadPullRequest(github, repo, listed.number);
    if (pr?.mergeable === "UNKNOWN") {
      core.warning(`Mergeability is still unknown for #${listed.number}.`);
    }
    let reasons = reasonsToDraft(pr, cutoff);
    if (!reasons.length) continue;
    if (!dryRun) {
      const fresh = await loadPullRequest(github, repo, listed.number);
      if (!fresh || !sameVersion(pr, fresh)) continue;
      pr = fresh;
      reasons = reasonsToDraft(pr, cutoff);
      if (!reasons.length) continue;
      await github.graphql(
        `mutation($id: ID!) {
        convertPullRequestToDraft(input: { pullRequestId: $id }) {
          pullRequest { id }
        }
      }`,
        { id: pr.id }
      );
      // Respect GitHub's secondary rate limits between writes.
      await setTimeout(1000);
      await github.rest.issues.createComment({
        ...repo,
        issue_number: pr.number,
        body:
          `Marked as draft after ${IDLE_DAYS} days without activity: ${reasons.join(
            "; "
          )}.\n\n` +
          "Please address these items and mark **Ready for review** when ready.",
      });
      await setTimeout(1000);
    }
    core.info(
      `${dryRun ? "Would draft" : "Drafted"} #${pr.number}: ${reasons.join(
        "; "
      )}`
    );
    results.push({ number: pr.number, reasons });
  }
  return results;
}

module.exports = { run };
