<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Release Process

For contributor-facing guidance on release branches and backports, see the
[Contributor Guide Release Management page](../../docs/source/contributor-guide/release_management.md).

This guide is for maintainers to create release candidates and run the release
process.

## Release Prerequisites

### Add git remote for `apache` repo

The instructions below assume the upstream git repo `git@github.com:apache/datafusion.git` in remote `apache`.

```shell
git remote add apache git@github.com:apache/datafusion.git
```

### Create GitHub Personal Access Token (PAT)

A personal access token (PAT) is needed for changelog automation script. If you
do not already have one, create a token with the `repo` access by navigating to
[GitHub Developer Settings] page, and [follow these steps].

[github developer settings]: https://github.com/settings/developers
[follow these steps]: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token

### Add GPG Public Key to SVN `KEYS` file

If you will be releasing the final tarball, your GPG public key must be present in the following SVN files:

- https://dist.apache.org/repos/dist/dev/datafusion/KEYS
- https://dist.apache.org/repos/dist/release/datafusion/KEYS

See instructions at https://infra.apache.org/release-signing.html#generate for generating keys.

Committers can add signing keys using the Subversion client and their ASF account:

```shell
$ svn co https://dist.apache.org/repos/dist/dev/datafusion
$ cd datafusion
$ editor KEYS # add your key here
$ svn ci KEYS # commit changes
```

Follow the instructions in the header of the KEYS file to append your key. Here is an example:

```shell
(gpg --list-sigs "John Doe" && gpg --armor --export "John Doe") >> KEYS
svn commit KEYS -m "Add key for John Doe"
```

## Release Process: Step by Step

As part of the Apache governance model, official releases consist of signed
source tarballs approved by the PMC.
We then publish the code in the approved artifacts to crates.io.

### 1. Create Release Branch

First create a new release branch from `main` in the `apache` repository.

For example, to create the `branch-50` branch for the `50.x.y` release series:

```shell
git fetch apache             # make sure we are up to date
git checkout apache/main     # checkout current latest development branch
git checkout -b branch-50    # create local branch
git push -u apache branch-50 # push branch to apache remote
```

### 2. Prepare PR to Update the Release Version

Manually update the DataFusion version in the root `Cargo.toml` to
reflect the new release version. Ensure `Cargo.lock` is updated accordingly by
running:

```shell
cargo check -p datafusion
```

Then commit the changes and create a PR targeting the release branch `branch-N`.

```shell
git commit -a -m 'Update version'
```

### 3. Protect Release Branch

To protect a release candidate branch from accidental merges, create PR against `main`:

```shell
git fetch apache && git checkout -b protect_branch_50
./dev/release/add-branch-protection.sh 50
```

The script will modify `.asf.yaml` and add following block:

```yaml
branch-50:
  required_pull_request_reviews:
    required_approving_review_count: 1
```

- Commit changes
- Push to `origin/protect_branch_50`
- Create a PR against `main`.
- Merge to `main`.
- Notify community in Discord/Slack that release branch is created

### 4. Backporting urgent changes

After release branch `branch-N` created, protected and got its version updated, please check if there are any backports expected from the community.
Please refer to [Backport Flow](../../docs/source/contributor-guide/release_management.md#backport-workflow) for more details.

Backports are important and sometimes unexpected, so please proceed to next release steps once all expected backports are applied.

### 5. Prepare PR to Update Changelog

Update the changelog in `dev/changelog/`. Each release has its
own file, such as `dev/changelog/50.0.0.md`, which should include all changes
since the previous release.

The changelog is generated using a Python script, which requires a GitHub
Personal Access Token (described in the prerequisites section) and the
`PyGitHub` library. First install the dev dependencies via `uv`:

```shell
uv sync
```

To generate the changelog, set the `GITHUB_TOKEN` environment variable and run
`./dev/release/generate-changelog.py` with two commit IDs or tags followed by
the release version. For example, to generate a changelog of all changes
between the `50.3.0` tag and `branch-51` for release `51.0.0`:

> [!NOTE]
>
> If you see errors such as the following, it is likely due to not setting
> the `GITHUB_TOKEN` environment variable.
>
> ```
> Request GET ... failed with 403: rate limit exceeded
> ```

```shell
export GITHUB_TOKEN=<your-token-here>
uv run ./dev/release/generate-changelog.py 50.3.0 branch-51 51.0.0 > dev/changelog/51.0.0.md
```

This script creates a changelog from GitHub PRs based on the labels associated with them as well as looking for
titles starting with `feat:`, `fix:`, or `docs:`.

Once the changelog is generated, run `prettier` to format the document:

```shell
prettier -w dev/changelog/51.0.0.md
```

Then commit the changes and create a PR targeting the release branch.

```shell
git commit -a -m 'Update changelog'
```

### 6. Prepare Release Candidate Artifacts

After the changelog updates merged to `branch-N`, you are ready to create release artifacts based off the
merged commit.

- You must be a committer to run these scripts because they upload to the
  Apache SVN distribution servers.
- If there are code changes between RCs, create and merge a changelog PR before
  creating the next RC.

#### Pick a Release Candidate (RC) number

Pick numbers in sequential order, with `1` for `rc1`, `2` for `rc2`, etc.

#### Create Git tag for the Release Candidate

While the official release artifacts are signed tarballs and zip files, we also
tag the commit it was created from for convenience and code archaeology. Release tags
look like `50.3.0`, and release candidate tags look like `50.3.0-rc1`. See [the list of existing
tags].

[the list of existing tags]: https://github.com/apache/datafusion/tags

Create and push the RC tag, for example, to create the `50.3.0-rc1` tag from `branch-50`, use:

```shell
git fetch apache
git tag 50.3.0-rc1 apache/branch-50
git push apache 50.3.0-rc1
```

Please make sure the format is correct, tools like Homebrew listens for tags and in case of malformed tags users would be notified for non-existent version

#### Create, Sign, and Upload Artifacts

Run the `create-tarball.sh` script with the `<version>` tag and `<rc>` number you determined in previous steps:

For example, to create the `50.3.0-rc1` artifacts:

```shell
GITHUB_TOKEN=<TOKEN> ./dev/release/create-tarball.sh 50.3.0 1
```

The `create-tarball.sh` script

1. Creates and uploads all release candidate artifacts to the [datafusion
   dev](https://dist.apache.org/repos/dist/dev/datafusion) location on the
   Apache distribution SVN server

2. Provides you an email template to
   send to `dev@datafusion.apache.org` for release voting.

### 7. Vote on Release Candidate Artifacts

Send the email output from the script to `dev@datafusion.apache.org`.

In order to publish the release on crates.io, it must be "official." To become
official, it needs at least three PMC members to vote +1 on it and no -1 votes.
The vote must remain open for at least 72 hours to give everyone a chance to
review the release candidate.

#### Verifying Release Candidates

`dev/release/verify-release-candidate.sh` is a script in this repository that can assist in the verification process. Run it like this:

```shell
./dev/release/verify-release-candidate.sh 50.3.0 1
```

#### If Changes Are Requested

If the release is not approved or urgent backports requested, please start over from [here](#4-backporting-urgent-changes)

#### If the Vote Passes: Announce the Result

Call the vote on the Datafusion dev list by replying to the RC voting thread. The
reply should have a new subject constructed by adding the `[RESULT]` prefix to the
old subject line.

Sample announcement template:

```
The vote has passed with <NUMBER> +1 votes. Thank you to all who helped
with the release verification.
```

### 8. Finalize the Release

NOTE: steps in this section can only be done by PMC members after release is approved.

Move artifacts to the release location in SVN, e.g.
https://dist.apache.org/repos/dist/release/datafusion/datafusion-50.3.0/, using
the `release-tarball.sh` script:

```shell
./dev/release/release-tarball.sh 50.3.0 1
```

Congratulations! The release is now official!

### 9. Create Release git tags

Tag the same release candidate commit with the final release tag

```shell
git checkout 50.3.0-rc1
git tag 50.3.0
git push apache 50.3.0
```

### 10. Publish on Crates.io

Only approved releases of the tarball should be published to
crates.io, in order to conform to Apache Software Foundation
governance standards.

An Arrow committer can publish this crate after an official project release has
been made to crates.io using the following instructions.

Follow [these
instructions](https://doc.rust-lang.org/cargo/reference/publishing.html) to
create an account and login to crates.io before asking to be added as an owner
to all DataFusion crates.

Download and unpack the official release tarball

Verify that the Cargo.toml in the tarball contains the correct version
(e.g. `version = "50.3.0"`) and then publish the crates by running the following commands

```shell
(cd datafusion/common && cargo publish)
(cd datafusion/common-runtime && cargo publish)
(cd datafusion/doc && cargo publish)
(cd datafusion/expr-common && cargo publish)
(cd datafusion/macros && cargo publish)
(cd datafusion/proto-common && cargo publish)
(cd datafusion/physical-expr-common && cargo publish)
(cd datafusion/functions-aggregate-common && cargo publish)
(cd datafusion/functions-window-common && cargo publish)
(cd datafusion/expr && cargo publish)
(cd datafusion/execution && cargo publish)
(cd datafusion/functions && cargo publish)
(cd datafusion/physical-expr && cargo publish)
(cd datafusion/functions-aggregate && cargo publish)
(cd datafusion/functions-window && cargo publish)
(cd datafusion/physical-expr-adapter && cargo publish)
(cd datafusion/functions-nested && cargo publish)
(cd datafusion/physical-plan && cargo publish)
(cd datafusion/session && cargo publish)
(cd datafusion/sql && cargo publish)
(cd datafusion/datasource && cargo publish)
(cd datafusion/optimizer && cargo publish)
(cd datafusion/catalog && cargo publish)
(cd datafusion/datasource-arrow && cargo publish)
(cd datafusion/datasource-avro && cargo publish)
(cd datafusion/datasource-csv && cargo publish)
(cd datafusion/datasource-json && cargo publish)
(cd datafusion/pruning && cargo publish)
(cd datafusion/datasource-parquet && cargo publish)
(cd datafusion/functions-table && cargo publish)
(cd datafusion/physical-optimizer && cargo publish)
(cd datafusion/catalog-listing && cargo publish)
(cd datafusion/core && cargo publish)
(cd datafusion-cli && cargo publish)
(cd datafusion/proto && cargo publish)
(cd datafusion/spark && cargo publish)
(cd datafusion/substrait && cargo publish)
(cd datafusion/ffi && cargo publish)
(cd datafusion/sqllogictest && cargo publish)
```

Crates.io publishing depends on crates dependency tree, this list might contain wrong order.
If it happens crates.io fails with wrong dependency message like below, just rerun all publishing commands.

```shell
error: failed to prepare local package for uploading

Caused by:
  failed to select a version for the requirement `datafusion-proto = "^53.1.0"`
  candidate versions found which didn't match: 53.0.0, 52.5.0, 52.4.0, ...
  location searched: crates.io index
  required by package `datafusion-ffi v53.1.0 (/private/tmp/apache-datafusion-53.1.0/datafusion/ffi)`
```

### Publish datafusion-cli on Homebrew

Note: [`datafusion` formula](https://formulae.brew.sh/formula/datafusion) is [updated automatically](https://github.com/Homebrew/homebrew-core/pulls?q=is%3Apr+datafusion+is%3Aclosed),
so no action is needed.

### 11. Sync Changelog and Version to main

To sync changelog version and create PR against `main`:

```shell
git fetch apache && git checkout -b sync_change_log_version
```

- Cherry-pick or patch the version updates from step 2
- Cherry-pick or patch the changelog updates from step 5
- Commit changes, `git commit -a -m 'Sync changelog and version to main'`
- Push to `origin/sync_change_log_version`
- Create a PR against `main`.
- Merge to `main`.

### 12. Add the release to Apache Reporter

When you have published the release, please help the project by adding the release to
[Apache Reporter](https://reporter.apache.org/). The reporter system should
send you a reminder email, but in case you miss it, you can add
the release to https://reporter.apache.org/addrelease.html?datafusion following
the examples from previous releases.

The release information is used to generate a template for a board report (see example from Apache Arrow project
[here](https://github.com/apache/arrow/pull/14357)).

### 13. Delete Old RCs and Releases

See the ASF documentation on [when to archive](https://www.apache.org/legal/release-policy.html#when-to-archive)
for more information.

Release candidates should be deleted once the release is published.

To get a list of DataFusion release candidates:

```shell
svn ls https://dist.apache.org/repos/dist/dev/datafusion
```

To delete a release candidate:

```shell
svn delete -m "delete old DataFusion RC" https://dist.apache.org/repos/dist/dev/datafusion/apache-datafusion-50.0.0-rc1/
```

#### Delete old releases from `release` SVN

Only the latest release should be available. Delete old releases after
publishing the new release.

To get a list of DataFusion releases:

```shell
svn ls https://dist.apache.org/repos/dist/release/datafusion
```

To delete a release:

```shell
svn delete -m "delete old DataFusion release" https://dist.apache.org/repos/dist/release/datafusion/datafusion-50.0.0
```
