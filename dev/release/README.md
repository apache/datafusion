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

## Branching Policy

- When we prepare a new release, we create a release branch, such as `branch-18` in the Apache repository (not in a fork)
- We update the crate version and generate the changelog in this branch and create a PR against the main branch
- Once the PR is approved and merged, we tag the rc in the release branch, and release from the release branch
- Bug fixes can be merged to the release branch and patch releases can be created from the release branch

#### How to add changes to `branch-*` branch?

If you would like to propose your change for inclusion in a release branch

1. follow normal workflow to create PR to `main` branch and wait for its approval and merges.
2. after PR is squash merged to `main`, branch from most recent release branch (e.g. `branch-18`), cherry-pick the commit and create a PR to release branch.

## Prerequisite

- Have upstream git repo `git@github.com:apache/arrow-datafusion.git` add as git remote `apache`.
- Created a personal access token in GitHub for changelog automation script.
  - Github PAT should be created with `repo` access
- Make sure your signing key is added to the following files in SVN:
  - https://dist.apache.org/repos/dist/dev/arrow/KEYS
  - https://dist.apache.org/repos/dist/release/arrow/KEYS

### How to add signing key

See instructions at https://infra.apache.org/release-signing.html#generate for generating keys.

Committers can add signing keys in Subversion client with their ASF account. e.g.:

```bash
$ svn co https://dist.apache.org/repos/dist/dev/arrow
$ cd arrow
$ editor KEYS
$ svn ci KEYS
```

Follow the instructions in the header of the KEYS file to append your key. Here is an example:

```bash
(gpg --list-sigs "John Doe" && gpg --armor --export "John Doe") >> KEYS
svn commit KEYS -m "Add key for John Doe"
```

## Process Overview

As part of the Apache governance model, official releases consist of signed
source tarballs approved by the PMC.

We then use the code in the approved artifacts to release to crates.io and
PyPI.

### Change Log

We maintain a `CHANGELOG.md` so our users know what has been changed between releases.

You will need a GitHub Personal Access Token for the following steps. Follow
[these instructions](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
to generate one if you do not already have one.

The changelog is generated using a Python script. There is a dependency on `PyGitHub`, which can be installed using pip:

```bash
pip3 install PyGitHub
```

Run the following command to generate the changelog content.

```bash
$ GITHUB_TOKEN=<TOKEN> ./dev/release/generate-changelog.py apache/arrow-datafusion 24.0.0 HEAD > dev/changelog/25.0.0.md
```

This script creates a changelog from GitHub PRs based on the labels associated with them as well as looking for
titles starting with `feat:`, `fix:`, or `docs:` . The script will produce output similar to:

```
Fetching list of commits between 24.0.0 and HEAD
Fetching pull requests
Categorizing pull requests
Generating changelog content
```

This process is not fully automated, so there are some additional manual steps:

- Add the ASF header to the generated file
- Add a link to this changelog from the top-level `/datafusion/CHANGELOG.md`
- Add the following content (copy from the previous version's changelog and update as appropriate:

```
## [24.0.0](https://github.com/apache/arrow-datafusion/tree/24.0.0) (2023-05-06)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/23.0.0...24.0.0)
```

## Prepare release commits and PR

Prepare a PR to update `CHANGELOG.md` and versions to reflect the planned
release.

See [#801](https://github.com/apache/arrow-datafusion/pull/801) for an example.

Here are the commands that could be used to prepare the `5.1.0` release:

### Update Version

Checkout the main commit to be released

```
git fetch apache
git checkout apache/main
```

Update datafusion version in `datafusion/Cargo.toml` to `5.1.0`:

```
./dev/update_datafusion_versions.py 5.1.0
```

Lastly commit the version change:

```
git commit -a -m 'Update version'
```

## Prepare release candidate artifacts

After the PR gets merged, you are ready to create release artifacts based off the
merged commit.

(Note you need to be a committer to run these scripts as they upload to the apache svn distribution servers)

### Pick a Release Candidate (RC) number

Pick numbers in sequential order, with `0` for `rc0`, `1` for `rc1`, etc.

### Create git tag for the release:

While the official release artifacts are signed tarballs and zip files, we also
tag the commit it was created for convenience and code archaeology.

Using a string such as `5.1.0` as the `<version>`, create and push the tag by running these commands:

```shell
git fetch apache
git tag <version>-<rc> apache/main
# push tag to Github remote
git push apache <version>
```

### Create, sign, and upload artifacts

Run `create-tarball.sh` with the `<version>` tag and `<rc>` and you found in previous steps:

```shell
GH_TOKEN=<TOKEN> ./dev/release/create-tarball.sh 5.1.0 0
```

The `create-tarball.sh` script

1. creates and uploads all release candidate artifacts to the [arrow
   dev](https://dist.apache.org/repos/dist/dev/arrow) location on the
   apache distribution svn server

2. provide you an email template to
   send to dev@arrow.apache.org for release voting.

### Vote on Release Candidate artifacts

Send the email output from the script to dev@arrow.apache.org. The email should look like

```
To: dev@arrow.apache.org
Subject: [VOTE][DataFusion] Release Apache Arrow DataFusion 5.1.0 RC0

Hi,

I would like to propose a release of Apache Arrow DataFusion Implementation,
version 5.1.0.

This release candidate is based on commit: a5dd428f57e62db20a945e8b1895de91405958c4 [1]
The proposed release artifacts and signatures are hosted at [2].
The changelog is located at [3].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release.

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow DataFusion 5.1.0
[ ] +0
[ ] -1 Do not release this as Apache Arrow DataFusion 5.1.0 because...

Here is my vote:

+1

[1]: https://github.com/apache/arrow-datafusion/tree/a5dd428f57e62db20a945e8b1895de91405958c4
[2]: https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-datafusion-5.1.0
[3]: https://github.com/apache/arrow-datafusion/blob/a5dd428f57e62db20a945e8b1895de91405958c4/CHANGELOG.md
```

For the release to become "official" it needs at least three PMC members to vote +1 on it.

### Verifying Release Candidates

The `dev/release/verify-release-candidate.sh` is a script in this repository that can assist in the verification process. Run it like:

```
./dev/release/verify-release-candidate.sh 5.1.0 0
```

#### If the release is not approved

If the release is not approved, fix whatever the problem is, merge changelog
changes into main if there is any and try again with the next RC number.

## Finalize the release

NOTE: steps in this section can only be done by PMC members.

### After the release is approved

Move artifacts to the release location in SVN, e.g.
https://dist.apache.org/repos/dist/release/arrow/arrow-datafusion-5.1.0/, using
the `release-tarball.sh` script:

```shell
./dev/release/release-tarball.sh 5.1.0 0
```

Congratulations! The release is now official!

### Create release git tags

Tag the same release candidate commit with the final release tag

```
git co apache/5.1.0-rc0
git tag 5.1.0
git push apache 5.1.0
```

### Publish on Crates.io

Only approved releases of the tarball should be published to
crates.io, in order to conform to Apache Software Foundation
governance standards.

An Arrow committer can publish this crate after an official project release has
been made to crates.io using the following instructions.

Follow [these
instructions](https://doc.rust-lang.org/cargo/reference/publishing.html) to
create an account and login to crates.io before asking to be added as an owner
of the following crates:

- [datafusion](https://crates.io/crates/datafusion)
- [datafusion-cli](https://crates.io/crates/datafusion-cli)
- [datafusion-common](https://crates.io/crates/datafusion-common)
- [datafusion-expr](https://crates.io/crates/datafusion-expr)
- [datafusion-physical-expr](https://crates.io/crates/datafusion-physical-expr)
- [datafusion-proto](https://crates.io/crates/datafusion-proto)
- [datafusion-execution](https://crates.io/crates/datafusion-execution)
- [datafusion-physical-plan](https://crates.io/crates/datafusion-physical-plan)
- [datafusion-sql](https://crates.io/crates/datafusion-sql)
- [datafusion-optimizer](https://crates.io/crates/datafusion-optimizer)
- [datafusion-substrait](https://crates.io/crates/datafusion-substrait)

Download and unpack the official release tarball

Verify that the Cargo.toml in the tarball contains the correct version
(e.g. `version = "5.1.0"`) and then publish the crates by running the script `release-crates.sh`
in a directory extracted from the source tarball that was voted on. Note that this script doesn't
work if run in a Git repo.

Alternatively the crates can be published one at a time with the following commands. Crates need to be
published in the correct order as shown in this diagram.

![](crate-deps.svg)

_To update this diagram, manually edit the dependencies in [crate-deps.dot](crate-deps.dot) and then run:_

```bash
dot -Tsvg dev/release/crate-deps.dot > dev/release/crate-deps.svg
```

```shell
(cd datafusion/common && cargo publish)
(cd datafusion/expr && cargo publish)
(cd datafusion/sql && cargo publish)
(cd datafusion/physical-expr && cargo publish)
(cd datafusion/optimizer && cargo publish)
(cd datafusion/execution && cargo publish)
(cd datafusion/physical-plan && cargo publish)
(cd datafusion/core && cargo publish)
(cd datafusion/proto && cargo publish)
(cd datafusion/substrait && cargo publish)
```

The CLI needs a `--no-verify` argument because `build.rs` generates source into the `src` directory.

```shell
(cd datafusion-cli && cargo publish --no-verify)
```

### Publish datafusion-cli on Homebrew

Run `publish_homebrew.sh` to publish `datafusion-cli` on Homebrew. In order to do so it is necessary to
fork the `homebrew-core` repo https://github.com/Homebrew/homebrew-core/, have Homebrew installed on your
macOS/Linux/WSL2 and properly configured and have a Github Personal Access Token that has permission to file pull requests in the `homebrew-core` repo.

#### Fork the `homebrew-core` repo

Go to https://github.com/Homebrew/homebrew-core/ and fork the repo.

#### Install and configure Homebrew

Please visit https://brew.sh/ to obtain Homebrew. In addition to that please check out https://docs.brew.sh/Homebrew-on-Linux if you are on Linux or WSL2.

Before running the script make sure that you can run the following command in your bash to make sure
that `brew` has been installed and configured properly:

```bash
brew --version
```

#### Create a Github Personal Access Token

To create a Github Personal Access Token, please visit https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token for instructions.

- Make sure to select either **All repositories** or **Only selected repositories** so that you have access to **Repository permissions**.
- If you only use the token for selected repos make sure you include your
  fork of `homebrew-core` in the list of repos under **Selected repositories**.
- Make sure to have **Read and write** access enabled for pull requests in your **Repository permissions**.

After all of the above is complete execute the following command:

```bash
dev/release/publish_homebrew.sh <version> <github-user> <github-token> <homebrew-default-branch-name>
```

Note that sometimes someone else has already submitted a PR to update the datafusion formula in homebrew.
In this case you will get an error with a message that your PR is a duplicate of an existing one. In this
case no further action is required.

Alternatively manually submit a simple PR to update tag and commit hash for the datafusion
formula in homebrew-core. Here is an example PR:
https://github.com/Homebrew/homebrew-core/pull/89562.

### Call the vote

Call the vote on the Arrow dev list by replying to the RC voting thread. The
reply should have a new subject constructed by adding `[RESULT]` prefix to the
old subject line.

Sample announcement template:

```
The vote has passed with <NUMBER> +1 votes. Thank you to all who helped
with the release verification.
```

You can include mention crates.io and PyPI version URLs in the email if applicable.

```
We have published new versions of DataFusion to crates.io:

https://crates.io/crates/datafusion/28.0.0
https://crates.io/crates/datafusion-cli/28.0.0
https://crates.io/crates/datafusion-common/28.0.0
https://crates.io/crates/datafusion-expr/28.0.0
https://crates.io/crates/datafusion-optimizer/28.0.0
https://crates.io/crates/datafusion-physical-expr/28.0.0
https://crates.io/crates/datafusion-proto/28.0.0
https://crates.io/crates/datafusion-sql/28.0.0
https://crates.io/crates/datafusion-execution/28.0.0
https://crates.io/crates/datafusion-substrait/28.0.0
```

### Add the release to Apache Reporter

Add the release to https://reporter.apache.org/addrelease.html?arrow with a version name prefixed with `RS-DATAFUSION-`,
for example `RS-DATAFUSION-14.0.0`.

The release information is used to generate a template for a board report (see example
[here](https://github.com/apache/arrow/pull/14357)).

### Delete old RCs and Releases

See the ASF documentation on [when to archive](https://www.apache.org/legal/release-policy.html#when-to-archive)
for more information.

#### Deleting old release candidates from `dev` svn

Release candidates should be deleted once the release is published.

Get a list of DataFusion release candidates:

```bash
svn ls https://dist.apache.org/repos/dist/dev/arrow | grep datafusion
```

Delete a release candidate:

```bash
svn delete -m "delete old DataFusion RC" https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-datafusion-7.1.0-rc1/
```

#### Deleting old releases from `release` svn

Only the latest release should be available. Delete old releases after publishing the new release.

Get a list of DataFusion releases:

```bash
svn ls https://dist.apache.org/repos/dist/release/arrow | grep datafusion
```

Delete a release:

```bash
svn delete -m "delete old DataFusion release" https://dist.apache.org/repos/dist/release/arrow/arrow-datafusion-7.0.0
```

### Publish the User Guide to the Arrow Site

- Run the `build.sh` in the `docs` directory from the release tarball.
- Clone the [arrow-site](https://github.com/apache/arrow-site) repository
- Checkout the `asf-site` branch
- Copy content from `docs/build/html/*` to the `datafusion` directory in arrow-site
- Create a PR against the `asf-site` branch ([example](https://github.com/apache/arrow-site/pull/237))
- Once the PR is merged, the content will be published to https://arrow.apache.org/datafusion/ by GitHub Pages (this
  can take some time).

### Optional: Write a blog post announcing the release

We typically crowdsource release announcements by collaborating on a Google document, usually starting
with a copy of the previous release announcement.

Run the following commands to get the number of commits and number of unique contributors for inclusion in the blog post.

```bash
git log --pretty=oneline 10.0.0..11.0.0 datafusion datafusion-cli datafusion-examples | wc -l
git shortlog -sn 10.0.0..11.0.0 datafusion datafusion-cli datafusion-examples | wc -l
```

Once there is consensus on the contents of the post, create a PR to add a blog post to the
[arrow-site](https://github.com/apache/arrow-site) repository. Note that there is no need for a formal
PMC vote on the blog post contents since this isn't considered to be a "release".

Here is an example blog post PR:

- https://github.com/apache/arrow-site/pull/217

Once the PR is merged, a GitHub action will publish the new blog post to https://arrow.apache.org/blog/.
