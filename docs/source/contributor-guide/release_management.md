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

# Release Management

This page describes DataFusion release branches and backports. For the
maintainer release guide, including release candidate artifacts, voting, and
publication, see [the release process README in `dev/release`].

## Overview

DataFusion typically has a major release about once per month, including
breaking API changes. Patch releases are made on an ad hoc basis, but we try to
avoid them because major releases are frequent.

New development happens on the [`main` branch]. Releases are made from release
branches named `branch-NN`, such as [`branch-50`] for the `50.x.y` release
series.

In general:

- New features land on [`main`]
- Patch releases are cut from the corresponding `branch-NN`
- Only targeted, low-risk fixes should be added to a release branch

Changes reach a release branch in one of two ways:

- (Most common) Fix the issue on `main` and then backport the merged change to the release branch
- Fix the issue on the release branch and then forward-port the change to `main`

Releases are coordinated in a GitHub issue, such as the
[release issue for 50.3.0]. If you think a fix should be included in a patch
release, discuss it on the relevant tracking issue first. You can also open the
backport PR first and then link it from the tracking issue.

To prepare for a new release series, maintainers:

- Create a new branch from `main`, such as `branch-50`, in the Apache repository
- Continue merging new features to `main`
- Prepare the release branch for release by updating versions, changelog content,
  and any additional release-specific fixes via the
  [Backport Workflow](#backport-workflow)
- Create release candidate artifacts from the release branch
- After approval, publish to crates.io, ASF distribution servers, and Git tags

## Backport Workflow

The usual workflow is:

1. Fix on `main` first, and merge the fix via a normal PR workflow.
2. Cherry-pick the merged commit onto the release branch.
3. Open a backport PR targeting the release branch (examples below).

- [Example backport PR]
- [Additional backport PR example]

### Inputs

To backport a change, gather the following information:

- Target branch, such as `apache/branch-52`
- The release tracking issue URL, such as https://github.com/apache/datafusion/issues/19692
- The original PR URL, such as https://github.com/apache/datafusion/pull/20192
- Optional explicit commit SHA to backport

### Apply the Backport

Start from the target release branch, create a dedicated backport branch, and
use `git cherry-pick`. For example, to backport PR #1234 to `branch-52` when
the commit SHA is `abc123`, run:

```bash
git checkout apache/branch-52
git checkout -b alamb/backport_1234
git cherry-pick abc123
```

### Test

Run tests as described in the [testing documentation].

### Open the PR

Create a PR against the release branch, not `main`, and prefix it with
`[branch-NN]` to show which release branch the backport targets. For example:

- `[branch-52] fix: validate inter-file ordering in eq_properties() (#20329)`

Use a PR description that links the tracking issue, original PR, and target
branch, for example:

```markdown
- Part of <tracking-issue-url>
- Closes <backport-issue-url> on <branch-name>

This PR:

- Backports <original-pr-url> from @<author> to the <branch-name> line
```

[`main` branch]: https://github.com/apache/datafusion/tree/main
[`branch-50`]: https://github.com/apache/datafusion/tree/branch-50
[the release process readme in `dev/release`]: https://github.com/apache/datafusion/blob/main/dev/release/README.md
[release issue for 50.3.0]: https://github.com/apache/datafusion/issues/18072
[example backport pr]: https://github.com/apache/datafusion/pull/18131
[additional backport pr example]: https://github.com/apache/datafusion/pull/20792
[testing documentation]: testing.md
