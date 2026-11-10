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

    await github.rest.issues.addLabels({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: context.payload.pull_request.number,
        labels: [target_version]
    });
};
