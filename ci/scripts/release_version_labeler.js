// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// Used by the release_version_labeler workflow

// A squash merge or a merge queue commit maps to the merged pull request. The
// pull request can lag the push by a few seconds, so retry.
async function mergedPulls({ github, context, sha }) {
    for (let attempt = 1; ; attempt++) {
        const { data } = await github.rest.repos.listPullRequestsAssociatedWithCommit({
            owner: context.repo.owner,
            repo: context.repo.repo,
            commit_sha: sha
        });
        const pulls = data.filter((pull) => pull.merged_at && pull.base.ref === 'main');
        if (pulls.length > 0 || attempt === 3) {
            return pulls;
        }
        await new Promise((resolve) => setTimeout(resolve, 5000));
    }
}

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

    // A push can include several merged pull requests.
    const shas = (context.payload.commits || []).map((commit) => commit.id);
    const numbers = new Set();
    for (const sha of shas.length ? shas : [context.sha]) {
        for (const pull of await mergedPulls({ github, context, sha })) {
            numbers.add(pull.number);
        }
    }
    if (numbers.size === 0) {
        console.log(`No merged pull request found for ${context.sha}`);
    }
    for (const number of numbers) {
        await github.rest.issues.addLabels({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: number,
            labels: [target_version]
        });
    }
};
