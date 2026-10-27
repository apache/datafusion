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

// Called on each new merge group and on a schedule. The queue's exact Git
// refs, rather than PR state or failing checks, determine which runs are stale.
const ACTIVE = new Set([
  "queued",
  "in_progress",
  "waiting",
  "pending",
  "requested",
]);
const CLEANUP_PATH = ".github/workflows/merge_queue_cleanup.yml";
const QUEUE_REF = /^gh-readonly-queue\/(.+)\/pr-[1-9][0-9]*-[0-9a-f]{40}$/;

function eligible(run, repo, currentRunId) {
  return (
    run.id !== currentRunId &&
    run.event === "merge_group" &&
    run.run_attempt === 1 &&
    ACTIVE.has(run.status) &&
    run.path?.split("@")[0] !== CLEANUP_PATH &&
    run.repository?.full_name?.toLowerCase() === repo.toLowerCase() &&
    run.head_repository?.full_name?.toLowerCase() === repo.toLowerCase() &&
    QUEUE_REF.test(run.head_branch) &&
    /^[0-9a-f]{40}$/.test(run.head_sha)
  );
}

async function refSha(github, repo, branch) {
  try {
    const { data } = await github.rest.git.getRef({
      ...repo,
      ref: `heads/${branch}`,
    });
    // Only a missing ref or a known, different commit proves obsolescence.
    // Unexpected responses and permission/server errors must not cancel CI.
    if (
      data.object?.type !== "commit" ||
      !/^[0-9a-f]{40}$/.test(data.object.sha)
    ) {
      throw new Error(`Invalid Git ref response for ${branch}`);
    }
    return data.object.sha;
  } catch (error) {
    if (error.status === 404) return null;
    throw error;
  }
}

// Filtered Actions listings are capped at 1,000 results even with pagination.
// Active merge-group runs stay far below the cap, so a full listing is an error.
async function listRuns(github, repo, status) {
  const params = { ...repo, event: "merge_group", status, per_page: 100 };
  const runs = [];
  for (let page = 1; ; page++) {
    const { data } = await github.rest.actions.listWorkflowRunsForRepo({
      ...params,
      page,
    });
    if (
      !Number.isInteger(data.total_count) ||
      data.total_count < 0 ||
      !Array.isArray(data.workflow_runs)
    ) {
      throw new Error("Invalid workflow run listing");
    }
    if (data.total_count >= 1000) {
      throw new Error(
        `${data.total_count} ${status} runs exceed the listing cap`
      );
    }
    runs.push(...data.workflow_runs);
    if (data.workflow_runs.length < 100) return runs;
  }
}

function reportError(core, context, message) {
  core.warning(message);
  // Maintenance errors must not mark a merge-group commit as failing CI.
  // Scheduled/manual scans still expose broken cleanup as workflow failures.
  if (context.eventName !== "merge_group") core.setFailed(message);
}

async function run({ github, context, core, dryRun = true }) {
  const repo = { owner: context.repo.owner, repo: context.repo.repo };
  const fullName = `${repo.owner}/${repo.repo}`;
  if (fullName !== "apache/datafusion") return [];

  // Finish pagination before cancelling: otherwise mutations shrink the active
  // result sets and can cause later pages to skip runs.
  const listed = new Map();
  try {
    for (const status of ACTIVE) {
      const runs = await listRuns(github, repo, status);
      for (const candidate of runs) listed.set(candidate.id, candidate);
    }
  } catch (error) {
    reportError(
      core,
      context,
      `Cannot list merge-group runs: ${error.message}`
    );
    return [];
  }

  const refs = new Map();
  const comparisons = new Map();
  const results = [];
  const errors = [];
  for (const candidate of listed.values()) {
    if (!eligible(candidate, fullName, context.runId)) continue;
    try {
      // Share the initial lookup among workflows for the same merge group.
      // Cache failures too: an outage must not multiply requests for this ref.
      if (!refs.has(candidate.head_branch)) {
        refs.set(
          candidate.head_branch,
          refSha(github, repo, candidate.head_branch)
        );
      }
      if ((await refs.get(candidate.head_branch)) === candidate.head_sha) {
        core.info(
          `Kept active group ${JSON.stringify({
            id: candidate.id,
            sha: candidate.head_sha,
            branch: candidate.head_branch,
            observed_at: new Date().toISOString(),
            trigger: context.eventName,
          })}`
        );
        continue;
      }

      const { data: current } = await github.rest.actions.getWorkflowRun({
        ...repo,
        run_id: candidate.id,
      });
      // A completed run or a newly requested rerun is no longer this candidate.
      if (
        !eligible(current, fullName, context.runId) ||
        current.head_branch !== candidate.head_branch ||
        current.head_sha !== candidate.head_sha ||
        current.run_attempt !== candidate.run_attempt
      )
        continue;

      const head = await refSha(github, repo, current.head_branch);
      if (head === current.head_sha) continue;
      // A successful merge removes the queue ref too. Preserve checks on
      // commits already in the target branch, including optional checks.
      // Workflows of one group share the comparison, failures included.
      const base = QUEUE_REF.exec(current.head_branch)[1];
      const basehead = `${base}...${current.head_sha}`;
      if (!comparisons.has(basehead)) {
        comparisons.set(
          basehead,
          github.rest.repos.compareCommitsWithBasehead({
            ...repo,
            basehead,
            per_page: 1,
          })
        );
      }
      const { data: comparison } = await comparisons.get(basehead);
      if (["identical", "behind"].includes(comparison.status)) continue;
      if (!["ahead", "diverged"].includes(comparison.status)) {
        throw new Error(`Invalid comparison status for ${current.head_sha}`);
      }
      const reason = head === null ? "queue ref removed" : "queue ref replaced";
      const result = {
        id: current.id,
        sha: current.head_sha,
        branch: current.head_branch,
        attempt: current.run_attempt,
        reason,
        observed_at: new Date().toISOString(),
        trigger: context.eventName,
      };
      const description = JSON.stringify(result);
      if (dryRun) {
        core.info(`Would cancel ${description}`);
      } else {
        try {
          await github.rest.actions.cancelWorkflowRun({
            ...repo,
            run_id: current.id,
          });
        } catch (error) {
          // A 409 can mean a completion race, but also an active run that
          // GitHub cannot cancel. Only suppress it after confirming the race.
          if (error.status === 409) {
            const { data: latest } = await github.rest.actions.getWorkflowRun({
              ...repo,
              run_id: current.id,
            });
            if (
              latest.status === "completed" ||
              latest.run_attempt !== current.run_attempt
            )
              continue;
          }
          throw error;
        }
        core.info(`Requested cancellation of ${description}`);
      }
      results.push(result);
    } catch (error) {
      // One failed read/cancellation must not leave other obsolete groups running.
      const message = `Run ${candidate.id}: ${error.message}`;
      core.warning(message);
      errors.push(message);
    }
  }
  if (errors.length)
    reportError(
      core,
      context,
      `${errors.length} merge queue cleanup errors; see warnings.`
    );
  return results;
}

module.exports = { run };
