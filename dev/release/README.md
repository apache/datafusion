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

## Sub-projects

The Datafusion repo contains 3 different releasable sub-projects: Datafusion, Ballista and Datafusion python binding.

We use Datafusion release to drive the release for the other sub-projects. As a
result, Datafusion version bump is required for every release while version
bumps for the Python binding and Ballista are optional. In other words, we can
release a new version of Datafusion without releasing a new version of the
Python binding or Ballista. On the other hand, releasing a new version of the
Python binding or Ballista always requires a new Datafusion version release.

## Branching

Datafusion currently only releases from the `master` branch. Given the project
is still in early development state, we are not maintaining an active stable
release backport branch.

## Prerequisite

- Have upstream git repo `git@github.com:apache/arrow-datafusion.git` add as git remote `apache`.
- Created a peronal access token in Github for changelog automation script.
  - Github PAT should be created with `repo` access
- Make sure your signing key is added to the following files in SVN:
  - https://dist.apache.org/repos/dist/dev/arrow/KEYS
  - https://dist.apache.org/repos/dist/release/arrow/KEYS

## Process Overview

As part of the Apache governance model, official releases consist of signed
source tarballs approved by the PMC.

We then use the code in the approved artifacts to release to crates.io and
PyPI.

### Change Log

We maintain `CHANGELOG.md` for each sub project so our users know what has been
changed between releases.

The CHANGELOG is managed automatically using
[update_change_log.sh](https://github.com/apache/arrow-datafusion/blob/master/dev/release/update_change_log.sh)

This script creates a changelog using github PRs and issues based on the labels
associated with them.

## Prepare release comimts and PR

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

If there is a ballista release, update versions in ballista Cargo.tomls, run

```
./dev/update_ballista_versions.py 0.5.0
```

If there is a datafusion python binding release, update versions in
`./python/Cargo.toml`.

Lastly commit the version change:

```
git commit -a -m 'Update version'
```

### Update CHANGELOG.md

Manully edit the base version tag argument in
`dev/release/update_change_log-{ballista,datafusion,python}.sh`. Commits
between the base verstion tag and the latest upstream master will be used to
populate the changelog content.

```bash
# create the changelog
CHANGELOG_GITHUB_TOKEN=<TOKEN> ./dev/release/update_change_log-all.sh
# review change log / edit issues and labels if needed, rerun until you are happy with the result
git commit -a -m 'Create changelog for release'
```

You can add `invalid` or `development-process` label to exclude items from
release notes. Add `datafusion`, `ballista` and `python` labels to group items
into each sub-project's change log.

Send a PR to get these changes merged into `master` branch. If new commits that
could change the change log content landed in the `master` branch before you
could merge the PR, you need to rerun the changelog update script to regenerate
the changelog and update the PR accordingly.

## Prepare release candidate artifacts

After the PR gets merged, you are ready to create releaes artifacts based off the
merged commit.

(Note you need to be a committer to run these scripts as they upload to the apache svn distribution servers)

### Pick an Release Candidate (RC) number

Pick numbers in sequential order, with `0` for `rc0`, `1` for `rc1`, etc.

### Create git tag for the release:

While the official release artifacts are signed tarballs and zip files, we also
tag the commit it was created for convenience and code archaeology.

Using a string such as `5.1.0` as the `<version>`, create and push the tag thusly:

```shell
git fetch apache
git tag <version>-<rc> apache/master
# push tag to Github remote
git push apache <version>
```

This should trigger the `Python Release Build` Github Action workflow for the
pushed tag. You can monitor the pipline run status at https://github.com/apache/arrow-datafusion/actions/workflows/python_build.yml.

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
Subject: [VOTE][Datafusion] Release Apache Arrow Datafusion 5.1.0 RC0

Hi,

I would like to propose a release of Apache Arrow Datafusion Implementation,
version 5.1.0.

This release candidate is based on commit: a5dd428f57e62db20a945e8b1895de91405958c4 [1]
The proposed release artifacts and signatures are hosted at [2].
The changelog is located at [3].

Please download, verify checksums and signatures, run the unit tests,
and vote on the release.

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow Datafusion 5.1.0
[ ] +0
[ ] -1 Do not release this as Apache Arrow Datafusion 5.1.0 because...

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

Congratulations! The release is now offical!

### Create release git tags

Tag the same release candidate commit with the final release tag

```
git co apache/5.1.0-rc0
git tag 5.1.0
git push apache 5.1.0
```

If there is a ballista release, also push the ballista tag

```
git tag ballista-0.5.0
git push apache ballista-0.5.0
```

If there is a datafusion python binding release, also push the python tag

```
git tag python-0.3.0
git push apache python-0.3.0
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
- [ballista](https://crates.io/crates/ballista)
- [ballista-core](https://crates.io/crates/ballista-core)
- [ballista-executor](https://crates.io/crates/ballista-executor)
- [ballista-scheduler](https://crates.io/crates/ballista-scheduler)

Download and unpack the official release tarball

Verify that the Cargo.toml in the tarball contains the correct version
(e.g. `version = "5.1.0"`) and then publish the crate with the
following commands

```shell
(cd datafusion && cargo publish)
```

If there is a ballista release, run

```shell
(cd ballista/rust/core && cargo publish)
(cd ballista/rust/executor && cargo publish)
(cd ballista/rust/scheduler && cargo publish)
(cd ballista/rust/client && cargo publish)
```

### Publish Python binding on PyPI

Only approved releases of the source tarball and wheels should be published to
PyPI, in order to conform to Apache Software Foundation governance standards.

First, download all official python release artifacts:

```shell
svn co https://dist.apache.org/repos/dist/release/arrow/arrow-datafusion-5.1.0/python ./python-artifacts
```

Use [twine](https://pypi.org/project/twine/) to perform the upload.

```shell
twine upload ./python-artifacts/*.{tar.gz,whl}
```

### Publish datafusion-cli on Homebrew and crates.io

For Homebrew, Send a simple PR to update tag and commit hash for the datafusion
formula in homebrew-core. Here is an example PR:
https://github.com/Homebrew/homebrew-core/pull/89562.

For crates.io, run

```shell
(cd datafusion-cli && cargo publish)
```

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
We have published new versions of datafusion and ballista to crates.io:

https://crates.io/crates/datafusion/5.0.0
https://crates.io/crates/ballista/0.5.0
https://crates.io/crates/ballista-core/0.5.0
https://crates.io/crates/ballista-executor/0.5.0
https://crates.io/crates/ballista-scheduler/0.5.0
```
