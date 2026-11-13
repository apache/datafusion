// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// Used by the release_version_labeler workflow
//
// A merged pull request gets the release version that this file names at its
// merge commit. Every push to `main` labels the pull requests merged in the
// last week that have no release label yet. That covers the pull requests of
// the push, and one that an earlier run missed, such as a merge whose push did
// not run the workflow because its commit message has a skip marker.

const WINDOW_DAYS = 7;
const RELEASE_LABEL = /^v\d+\.\d+\.\d+$/;
const SCRIPT_PATH = 'ci/scripts/release_version_labeler.js';
// dev/update_datafusion_versions.py rewrites every match of its pattern in this
// file. The quote is in a class here, so that only the constant below matches.
const TARGET_VERSION = /const target_version = [']([^']+)'/;

module.exports = async ({ github, context }) => {
    const target_version = 'v57.0.0';
    const { owner, repo } = context.repo;

    // The release version that this file names at a commit.
    async function versionAt(sha) {
        if (sha === context.sha) {
            return target_version;
        }
        const { data } = await github.rest.repos.getContent({ owner, repo, path: SCRIPT_PATH, ref: sha });
        const match = TARGET_VERSION.exec(Buffer.from(data.content, 'base64').toString());
        if (!match) {
            throw new Error(`No target version in ${SCRIPT_PATH} at ${sha}`);
        }
        return match[1];
    }

    // A run for another push can create the label first.
    async function createLabel(name) {
        try {
            await github.rest.issues.getLabel({ owner, repo, name });
        } catch (error) {
            if (error.status !== 404) {
                throw error;
            }
            try {
                await github.rest.issues.createLabel({ owner, repo, name, color: '222222' });
            } catch (error) {
                if (error.status !== 422) {
                    throw error;
                }
            }
        }
    }

    // Merging a pull request updates it, so every pull request merged after the
    // cutoff is listed before the first one updated before it.
    const cutoff = Date.now() - WINDOW_DAYS * 24 * 60 * 60 * 1000;
    const unlabeled = [];
    const pages = github.paginate.iterator(github.rest.pulls.list, {
        owner,
        repo,
        state: 'closed',
        base: 'main',
        sort: 'updated',
        direction: 'desc',
        per_page: 100
    });
    listing: for await (const { data } of pages) {
        for (const pull of data) {
            if (Date.parse(pull.updated_at) < cutoff) {
                break listing;
            }
            if (
                pull.merged_at &&
                Date.parse(pull.merged_at) >= cutoff &&
                !pull.labels.some((label) => RELEASE_LABEL.test(label.name))
            ) {
                unlabeled.push(pull);
            }
        }
    }
    if (unlabeled.length === 0) {
        console.log(`No pull request merged in the last ${WINDOW_DAYS} days needs a release label`);
    }

    // A failure must not cost the other pull requests their label, so log it
    // and throw the first one after labeling.
    const labels = new Set();
    const errors = [];
    for (const pull of unlabeled) {
        try {
            const version = await versionAt(pull.merge_commit_sha);
            if (!labels.has(version)) {
                await createLabel(version);
                labels.add(version);
            }
            await github.rest.issues.addLabels({ owner, repo, issue_number: pull.number, labels: [version] });
            console.log(`Labeled #${pull.number} with ${version}`);
        } catch (error) {
            console.log(`Labeling #${pull.number} failed: ${error}`);
            errors.push(error);
        }
    }
    if (errors.length > 0) {
        console.log(`${errors.length} pull request(s) failed, see above.`);
        throw errors[0];
    }
};
