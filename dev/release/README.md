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

DataFusion typically has major releases around once per month, including breaking API changes.

Patch releases are made on an adhoc basis, but we try and avoid them given the frequent major releases.

## Release Process Overview

New development happens on the `main` branch.
Releases are made from branches, e.g. `branch-50` for the `50.x.y` release series.

To prepare for a new release series, we:

- Create a new branch from `main`, such as `branch-50` in the Apache repository (not in a fork)
- Continue merging new features changes to `main` branch
- Prepare the release branch for release:
  - Update version numbers in `Cargo.toml` files and create `CHANGELOG.md`
  - Add additional changes to the release branch as needed
- When the code is ready, create GitHub tags release candidate (rc) artifacts from the release branch.
- After the release is approved, publish to [crates.io], the ASF distribution servers, and GitHub tags.

To add changes to the release branch, depending on the change we either:

- Fix the issue on `main` and then backport the change to the release branch (e.g. [#18129])
- Fix the issue on the release branch and then forward-port the change back to `main` (e.g.[#18057])

[crates.io]: https://crates.io/crates/datafusion
[#18129]: https://github.com/apache/datafusion/pull/18129
[#18057]: https://github.com/apache/datafusion/pull/18057

## Backporting (add changes) to `branch-*` branch

If you would like to propose your change for inclusion in a patch release, the
change must be applied to the relevant release branch. To do so please follow
these steps:

1. Find (or create) the issue for the incremental release ([example release issue]) and discuss the proposed change there with the maintainers.
2. Follow normal workflow to create PR to `main` branch and wait for its approval and merge.
3. After PR is squash merged to `main`, branch from most recent release branch (e.g. `branch-50`), cherry-pick the commit and create a PR targeting the release branch [example backport PR].

For example, to backport commit `12345` from `main` to `branch-50`:

```shell
git checkout branch-50
git checkout -b backport_to_50
git cherry-pick 12345 # your git commit hash
git push -u <your fork>
# make a PR as normal targeting branch-50, prefixed with [branch-50]
```

It is also acceptable to fix the issue directly on the release branch first
and then cherry-pick the change back to `main` branch in a new PR.

[example release issue]: https://github.com/apache/datafusion/issues/18072
[example backport pr]: https://github.com/apache/datafusion/pull/18131

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

First create a new release branch from `main` in the apache repository.

For example, to create the `branch-50` branch for the `50.x.y` release series:

```shell
git fetch apache             # make sure we are up to date
git checkout apache/main     # checkout current latest development branch
git checkout -b branch-50    # create local branch
git push -u apache branch-50 # push branch to apache remote
```

### 2. Add a protection to release candidate branch

To protect a release candidate branch from accidental merges, run:

```shell
./dev/release/add-branch-protection.sh 50
```

The script will modify `.asf.yaml` and add following block:

```yaml
branch-50:
  required_pull_request_reviews:
    required_approving_review_count: 1
```

- Create a PR.
- Merge to `main`.

### 3. Prepare PR to Update Changelog and the Release Version

First, prepare a PR to update the changelog and versions to reflect the planned
release. See [#18173](https://github.com/apache/datafusion/pull/18173) for an example.

#### Update Version Numbers

Manually update the DataFusion version in the root `Cargo.toml` to reflect the new release version.

Ensure Cargo.lock is updated accordingly by running:

```shell
cargo check -p datafusion
```

#### Changelog Generation

We maintain a [changelog] so our users know what has been changed between releases.

[changelog]: ../changelog

The changelog is generated using a Python script.

To run the script, you will need a GitHub Personal Access Token (described in the prerequisites section) and the `PyGitHub` library. First install the `PyGitHub` dependency via `pip`:

```shell
pip3 install PyGitHub
```

To generate the changelog, set the `GITHUB_TOKEN` environment variable and then run `./dev/release/generate-changelog.py`
providing two commit ids or tags followed by the version number of the release being created. For example,
to generate a change log of all changes between the `50.3.0` tag and `branch-51`, in preparation for release `51.0.0`:

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
./dev/release/generate-changelog.py 50.3.0 branch-51 51.0.0 > dev/changelog/51.0.0.md
```

This script creates a changelog from GitHub PRs based on the labels associated with them as well as looking for
titles starting with `feat:`, `fix:`, or `docs:`.

Once the change log is generated, run `prettier` to format the document:

```shell
prettier -w dev/changelog/51.0.0.md
```

#### Commit and PR

Then commit the changes and create a PR targeting the release branch.

```shell
git commit -a -m 'Update version'
```

Remember to merge any fixes back to `main` branch as well.

### 4. Prepare Release Candidate Artifacts

After the PR gets merged, you are ready to create release artifacts based off the
merged commit.

(Note you need to be a committer to run these scripts as they upload to the apache svn distribution servers)

#### Pick a Release Candidate (RC) number

Pick numbers in sequential order, with `1` for `rc1`, `2` for `rc2`, etc.

#### Create git Tag for the Release:

While the official release artifacts are signed tarballs and zip files, we also
tag the commit it was created for convenience and code archaeology. Release tags
have the format `<version>` (e.g. `38.0.0`), and release candidates have the
format `<version>-rc<rc>` (e.g. `38.0.0-rc0`). See [the list of existing
tags].

[the list of existing tags]: https://github.com/apache/datafusion/tags

Using a string such as `38.0.0` as the `<version>`, create and push the rc tag by running these commands:

```shell
git fetch apache
git tag <version>-<rc> apache/branch-X # create tag from the release branch
git push apache <version>-<rc>         # push tag to Github remote
```

For example, to create the `50.3.0-rc1 tag from `branch-50`:

```shell
git fetch apache
git tag 50.3.0-rc1 apache/branch-50
git push apache 50.3.0-rc1
```

#### Create, Sign, and Upload Artifacts

Run the `create-tarball.sh` script with the `<version>` tag and `<rc>` and you determined in previous steps:

For example, to create the `50.3.0-rc1` artifacts:

```shell
GH_TOKEN=<TOKEN> ./dev/release/create-tarball.sh 50.3.0 1
```

The `create-tarball.sh` script

1. Creates and uploads all release candidate artifacts to the [datafusion
   dev](https://dist.apache.org/repos/dist/dev/datafusion) location on the
   apache distribution SVN server

2. Provides you an email template to
   send to dev@datafusion.apache.org for release voting.

### 5. Vote on Release Candidate Artifacts

Send the email output from the script to dev@datafusion.apache.org.

In order to publish the release on crates.io, it must be "official". To become
official it needs at least three PMC members to vote +1 on it.

#### Verifying Release Candidates

The `dev/release/verify-release-candidate.sh` is a script in this repository that can assist in the verification process. Run it like:

```shell
./dev/release/verify-release-candidate.sh 50.3.0 1
```

#### If the Release is not Approved

If the release is not approved, fix whatever the problem is, merge changelog
changes into the release branch and try again with the next RC number.

Remember to merge any fixes back to `main` branch as well.

#### If the Release is Approved: Call the Vote

Call the vote on the Arrow dev list by replying to the RC voting thread. The
reply should have a new subject constructed by adding `[RESULT]` prefix to the
old subject line.

Sample announcement template:

```
The vote has passed with <NUMBER> +1 votes. Thank you to all who helped
with the release verification.
```

### 6. Finalize the Release

NOTE: steps in this section can only be done by PMC members.

#### After the release is approved

Move artifacts to the release location in SVN, e.g.
https://dist.apache.org/repos/dist/release/datafusion/datafusion-50.3.0/, using
the `release-tarball.sh` script:

```shell
./dev/release/release-tarball.sh 50.3.0 1
```

Congratulations! The release is now official!

### 7. Create Release git tags

Tag the same release candidate commit with the final release tag

```shell
git co apache/50.3.0-rc1
git tag 50.3.0
git push apache 50.3.0
```

### 8. Publish on Crates.io

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
(e.g. `version = "38.0.0"`) and then publish the crates by running the following commands

```shell
(cd datafusion/common && cargo publish)
(cd datafusion/expr-common && cargo publish)
(cd datafusion/physical-expr-common && cargo publish)
(cd datafusion/functions-aggregate-common && cargo publish)
(cd datafusion/functions-window-common && cargo publish)
(cd datafusion/doc && cargo publish)
(cd datafusion/expr && cargo publish)
(cd datafusion/macros && cargo publish)
(cd datafusion/execution && cargo publish)
(cd datafusion/functions && cargo publish)
(cd datafusion/physical-expr && cargo publish)
(cd datafusion/physical-expr-adapter && cargo publish)
(cd datafusion/functions-aggregate && cargo publish)
(cd datafusion/functions-window && cargo publish)
(cd datafusion/functions-nested && cargo publish)
(cd datafusion/sql && cargo publish)
(cd datafusion/optimizer && cargo publish)
(cd datafusion/common-runtime && cargo publish)
(cd datafusion/physical-plan && cargo publish)
(cd datafusion/pruning && cargo publish)
(cd datafusion/physical-optimizer && cargo publish)
(cd datafusion/session && cargo publish)
(cd datafusion/datasource && cargo publish)
(cd datafusion/catalog && cargo publish)
(cd datafusion/catalog-listing && cargo publish)
(cd datafusion/functions-table && cargo publish)
(cd datafusion/datasource-arrow && cargo publish)
(cd datafusion/datasource-csv && cargo publish)
(cd datafusion/datasource-json && cargo publish)
(cd datafusion/datasource-parquet && cargo publish)
(cd datafusion/core && cargo publish)
(cd datafusion/proto-common && cargo publish)
(cd datafusion/proto && cargo publish)
(cd datafusion/datasource-avro && cargo publish)
(cd datafusion/substrait && cargo publish)
(cd datafusion/ffi && cargo publish)
(cd datafusion-cli && cargo publish)
(cd datafusion/spark && cargo publish)
(cd datafusion/sqllogictest && cargo publish)
```

### Publish datafusion-cli on Homebrew

Note: [`datafusion` formula](https://formulae.brew.sh/formula/datafusion) is [updated automatically](https://github.com/Homebrew/homebrew-core/pulls?q=is%3Apr+datafusion+is%3Aclosed),
so no action is needed.

### 9: Add the release to Apache Reporter

When you have published the release, please help the project by adding the release to
[Apache Reporter](https://reporter.apache.org/). The reporter system should
send you a reminder email, but in case you miss it, you can add
the release to https://reporter.apache.org/addrelease.html?datafusion following
the examples from previous releases.

The release information is used to generate a template for a board report (see example from Apache Arrow project
[here](https://github.com/apache/arrow/pull/14357)).

### 10: Delete old RCs and Releases

See the ASF documentation on [when to archive](https://www.apache.org/legal/release-policy.html#when-to-archive)
for more information.

#### Deleting old release candidates from `dev` svn

Release candidates should be deleted once the release is published.

Get a list of DataFusion release candidates:

```shell
svn ls https://dist.apache.org/repos/dist/dev/datafusion
```

Delete a release candidate:

```shell
svn delete -m "delete old DataFusion RC" https://dist.apache.org/repos/dist/dev/datafusion/apache-datafusion-50.0.0-rc1/
```

#### Deleting old releases from `release` svn

Only the latest release should be available. Delete old releases after publishing the new release.

Get a list of DataFusion releases:

```shell
svn ls https://dist.apache.org/repos/dist/release/datafusion
```

Delete a release:

```shell
svn delete -m "delete old DataFusion release" https://dist.apache.org/repos/dist/release/datafusion/datafusion-50.0.0
```
