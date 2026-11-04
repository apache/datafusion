// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// Used by the release_version_labeler workflow

module.exports = async ({ github, context }) => {
    const target_version = 'v57.0.0';

    // create label first if not exists
    try {
        await github.rest.issues.getLabel({
        owner: context.repo.owner,
        repo: context.repo.repo,
        name: target_version
        });
    } catch (error) {
        if (error.status === 404) {
            await github.rest.issues.createLabel({
                owner: context.repo.owner,
                repo: context.repo.repo,
                name: target_version,
                color: '222222'
            });
        } else {
            throw error;
        }
    }

    // A squash merge or a merge queue commit maps to the merged pull request.
    // The pull request can lag the push by a few seconds, so retry.
    let pulls = [];
    for (let attempt = 0; attempt < 3 && pulls.length === 0; attempt++) {
        if (attempt > 0) {
            await new Promise((resolve) => setTimeout(resolve, 5000));
        }
        const { data } = await github.rest.repos.listPullRequestsAssociatedWithCommit({
            owner: context.repo.owner,
            repo: context.repo.repo,
            commit_sha: context.sha
        });
        pulls = data.filter((pull) => pull.merged_at && pull.base.ref === 'main');
    }
    if (pulls.length === 0) {
        throw new Error(`No merged pull request found for ${context.sha}`);
    }
    for (const pull of pulls) {
        await github.rest.issues.addLabels({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pull.number,
            labels: [target_version]
        });
    }
};
