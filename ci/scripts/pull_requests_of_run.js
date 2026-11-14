// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// Used by the labeler and fork_ci_status workflows, which run on `workflow_run`.
//
// `workflow_run.pull_requests` is empty for pull requests from forks, so find
// the open pull requests of the run's head branch. They are the current ones,
// which can be newer than the run's commit when pushes follow each other, so
// compare `head.sha` with `run.head_sha` where only that commit matters. The
// same branch can target several bases, so pass `base` to select one.
module.exports = async ({ github, context, base }) => {
    const run = context.payload.workflow_run;
    // The head repository is null when the fork was deleted.
    if (!run.head_repository) {
        return [];
    }
    const { data } = await github.rest.pulls.list({
        owner: context.repo.owner,
        repo: context.repo.repo,
        state: 'open',
        head: `${run.head_repository.owner.login}:${run.head_branch}`,
        ...(base && { base })
    });
    return data;
};
