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
const QUEUE_REF = /^gh-readonly-queue\/.+\/pr-[1-9][0-9]*-[0-9a-f]{40}$/;

function eligible(run, repo, currentRunId) {
  return (
    run.id !== currentRunId &&
    run.event === "merge_group" &&
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

async function run({ github, context, core, dryRun = true }) {
  const repo = { owner: context.repo.owner, repo: context.repo.repo };
  const fullName = `${repo.owner}/${repo.repo}`;
  if (fullName !== "apache/datafusion") return [];

  // Finish pagination before cancelling: otherwise mutations shrink the active
  // result sets and can cause later pages to skip runs.
  const listed = new Map();
  for (const status of ACTIVE) {
    const runs = await github.paginate(
      github.rest.actions.listWorkflowRunsForRepo,
      {
        ...repo,
        event: "merge_group",
        status,
        per_page: 100,
      }
    );
    for (const candidate of runs) listed.set(candidate.id, candidate);
  }

  const refs = new Map();
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
      if ((await refs.get(candidate.head_branch)) === candidate.head_sha)
        continue;

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
      const reason = head === null ? "queue ref removed" : "queue ref replaced";
      const description = `${current.id}: ${reason} (${current.head_branch}, ${current.head_sha})`;
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
      results.push({ id: current.id, reason });
    } catch (error) {
      // One failed read/cancellation must not leave other obsolete groups running.
      const message = `Run ${candidate.id}: ${error.message}`;
      core.warning(message);
      errors.push(message);
    }
  }
  if (errors.length)
    core.setFailed(
      `${errors.length} merge queue cleanup errors; see warnings.`
    );
  return results;
}

module.exports = { run };
