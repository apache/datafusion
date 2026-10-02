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

// Used by the release_version_labeler workflow

module.exports = async ({ github, context }) => {
    const target_version = 'v56.0.0';

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
