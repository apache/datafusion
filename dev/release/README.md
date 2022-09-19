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

## Branching

### Major Release

DataFusion typically has major releases from the `master` branch every 4 weeks, including breaking API changes.

### Minor Release

Starting v7.0.0, we are experimenting with maintaining an active stable release branch (e.g. `maint-7.x`). Every month, we will review the `maint-*` branch and prepare a minor release (e.g. v7.1.0) when necessary. A patch release (v7.0.1) can be requested on demand if it is urgent bug/security fix.

#### How to add changes to `maint-*` branch?

If you would like to propose your change for inclusion in the maintenance branch

1. follow normal workflow to create PR to `master` branch and wait for its approval and merges.
2. after PR is squash merged to `master`, branch from most recent maintenance branch (e.g. `maint-7-x`), cherry-pick the commit and create a PR to maintenance branch (e.g. `maint-7-x`).

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

We maintain a `CHANGELOG.md` so our users know what has been
changed between releases.

The CHANGELOG is managed automatically using
[update_change_log.sh](https://github.com/apache/arrow-datafusion/blob/master/dev/release/update_change_log.sh)

This script creates a changelog using GitHub PRs and issues based on the labels
associated with them.

## Prepare release commits and PR

Prepare a PR to update `CHANGELOG.md` and versions to reflect the planned
release.

See [#801](https://github.com/apache/arrow-datafusion/pull/801) for an example.

Here are the commands that could be used to prepare the `5.1.0` release:

### Update Version

Checkout the master commit to be released

```
git fetch apache
git checkout apache/master
```

Update datafusion version in `datafusion/Cargo.toml` to `5.1.0`:

```
./dev/update_datafusion_versions.py 5.1.0
```

Lastly commit the version change:

```
git commit -a -m 'Update version'
```

### Update CHANGELOG.md

Define release branch (e.g. `master`), base version tag (e.g. `7.0.0`) and future version tag (e.g. `8.0.0`). Commits between the base version tag and the release branch will be used to
populate the changelog content.

You will need a GitHub Personal Access Token for the following steps. Follow
[these instructions](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
to generate one if you do not already have one.

```bash
# create the changelog
CHANGELOG_GITHUB_TOKEN=<TOKEN> ./dev/release/update_change_log-datafusion.sh master 8.0.0 7.0.0
# review change log / edit issues and labels if needed, rerun until you are happy with the result
git commit -a -m 'Create changelog for release'
```

_If you see the error `"You have exceeded a secondary rate limit"` when running this script, try reducing the CPU
allocation to slow the process down and throttle the number of GitHub requests made per minute, by modifying the
value of the `--cpus` argument in the `update_change_log.sh` script._

You can add `invalid` or `development-process` label to exclude items from
release notes.

Send a PR to get these changes merged into `master` branch. If new commits that
could change the change log content landed in the `master` branch before you
could merge the PR, you need to rerun the changelog update script to regenerate
the changelog and update the PR accordingly.

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
git tag <version>-<rc> apache/master
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
changes into master if there is any and try again with the next RC number.

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
- [datafusion-jit](https://crates.io/crates/datafusion-jit)
- [datafusion-physical-expr](https://crates.io/crates/datafusion-physical-expr)
- [datafusion-proto](https://crates.io/crates/datafusion-proto)
- [datafusion-row](https://crates.io/crates/datafusion-row)

Download and unpack the official release tarball

Verify that the Cargo.toml in the tarball contains the correct version
(e.g. `version = "5.1.0"`) and then publish the crates with the
following commands. Crates need to be published in the correct order as shown in this diagram.

![](crate-deps.svg)

_To update this diagram, manually edit the dependencies in [crate-deps.dot](crate-deps.dot) and then run:_

```bash
dot -Tsvg dev/release/crate-deps.dot > dev/release/crate-deps.svg
```

```shell
(cd datafusion/common && cargo publish)
(cd datafusion/expr && cargo publish)
(cd datafusion/sql && cargo publish)
(cd datafusion/jit && cargo publish)
(cd datafusion/row && cargo publish)
(cd datafusion/physical-expr && cargo publish)
(cd datafusion/optimizer && cargo publish)
(cd datafusion/core && cargo publish)
(cd datafusion/proto && cargo publish)
```

The CLI needs a `--no-verify` argument because `build.rs` generates source into the `src` directory.

```shell
(cd datafusion-cli && cargo publish --no-verify)
```

### Publish datafusion-cli on Homebrew

For Homebrew, Send a simple PR to update tag and commit hash for the datafusion
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

https://crates.io/crates/datafusion/8.0.0
https://crates.io/crates/datafusion-cli/8.0.0
https://crates.io/crates/datafusion-common/8.0.0
https://crates.io/crates/datafusion-expr/8.0.0
https://crates.io/crates/datafusion-jit/8.0.0
https://crates.io/crates/datafusion-optimizer/8.0.0
https://crates.io/crates/datafusion-physical-expr/8.0.0
https://crates.io/crates/datafusion-proto/8.0.0
https://crates.io/crates/datafusion-row/8.0.0
https://crates.io/crates/datafusion-sql/8.0.0
```

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
