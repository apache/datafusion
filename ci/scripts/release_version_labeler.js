// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// Used by the release_version_labeler workflow

// A push can have many commits. Look them up this many at a time, to stay well
// below GitHub's limit of 100 concurrent requests.
const BATCH_SIZE = 10;

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

    // A push can include several merged pull requests. Look the commits up a
    // batch at a time, so the retries of commits without one overlap. A failed
    // lookup or label must not cost the other pull requests their label, so
    // log every error and throw the first one after labeling.
    const pushed = (context.payload.commits || []).map((commit) => commit.id);
    const shas = pushed.length ? pushed : [context.sha];
    const numbers = new Set();
    const errors = [];
    for (let i = 0; i < shas.length; i += BATCH_SIZE) {
        const batch = shas.slice(i, i + BATCH_SIZE);
        const settled = await Promise.allSettled(
            batch.map((sha) => mergedPulls({ github, context, sha }))
        );
        settled.forEach((result, index) => {
            if (result.status === 'fulfilled') {
                result.value.forEach((pull) => numbers.add(pull.number));
            } else {
                console.log(`Looking up ${batch[index]} failed: ${result.reason}`);
                errors.push(result.reason);
            }
        });
    }
    if (numbers.size === 0 && errors.length === 0) {
        console.log(`No merged pull request found for ${shas.join(', ')}`);
    }
    for (const number of numbers) {
        try {
            await github.rest.issues.addLabels({
                owner: context.repo.owner,
                repo: context.repo.repo,
                issue_number: number,
                labels: [target_version]
            });
        } catch (error) {
            console.log(`Labeling #${number} failed: ${error}`);
            errors.push(error);
        }
    }
    if (errors.length > 0) {
        console.log(`${errors.length} request(s) failed, see above.`);
        throw errors[0];
    }
};
