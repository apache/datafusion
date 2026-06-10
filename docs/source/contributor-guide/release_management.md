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

Releases are coordinated using GitHub issues. Each planned release is listed in
the [DataFusion Releases tracking issue], and each release is coordinated in a
dedicated issue, such as the [release issue for 50.3.0]. If you think a fix
should be included in a patch release, discuss it on the relevant tracking issue
or open a backport PR and link it there.

To prepare for a new release series, maintainers:

- Create a new branch from `main`, such as `branch-50`, in the Apache repository
- Continue merging new features to `main`
- Prepare the release branch for release by updating versions, changelog content,
  and any additional release-specific fixes via the
  [Backport Workflow](#backport-workflow)
- Create release candidate artifacts from the release branch
- After approval, publish to crates.io, ASF distribution servers, and Git tags

## Backport Criteria

A release branch is a stabilization branch for an imminent or recent patch
release. The bar for landing a change on a release branch is therefore
_higher_ than the bar for landing on `main`, not lower. These criteria define
what is eligible for backport; the [Backport Workflow](#backport-workflow)
below describes the mechanics.

DataFusion follows Cargo SemVer, with breaking changes allowed at major
version boundaries — see the [API health policy] for the full framing of
public Rust and SQL API stability. Patch releases (`x.y.z`, `z ≥ 1`) carry
fixes only and never introduce new features or breaking changes.

### Eligible for backport

- **Security fixes.** Fixes for known or reported security issues should be
  backported to every actively maintained release branch.
- **Correctness fixes.** Fixes for queries that produce incorrect results,
  panics, data loss, or crashes. If the fix itself changes user-visible SQL
  semantics to make a wrong result right, follow [Behavior changes] below.
- **Stability and regression fixes.** Fixes for regressions introduced in the
  current release line, hangs, deadlocks, memory leaks, or other availability
  issues.
- **Build, CI, and test fixes** required to keep the branch buildable and
  releasable.
- **Documentation fixes** for behavior already in the release. Documentation
  for behavior that exists only on `main` does not belong on a release branch.

### Not recommended for backport

- **New features**, including new SQL functions, new optimizer rules, new
  configuration options, new public APIs, and new file-format support. Land
  on `main` and ship in the next major release.
- **Breaking API changes** of any kind, Rust or SQL. DataFusion makes
  breaking changes only at major version boundaries — see [API health policy].
- **Refactors and cleanup** that do not fix a bug, even if they are correct.
- **Performance improvements** that are not also correctness or stability
  fixes. Land on `main`.
- **Dependency upgrades**, except when the upgrade itself is the security or
  correctness fix and there is no narrower alternative.

### Behavior changes

A "behavior change" is any fix that alters user-visible results: SQL
semantics (values, ordering, types, null handling), error messages that
downstream users may rely on, plan output, or default configuration values.

Behavior-changing fixes need extra scrutiny on a release branch because
users upgrading between patch versions do not expect their queries to start
returning different results. When proposing one for backport, state on the
release tracking issue _why_ the change should ship in this patch release
rather than wait for the next major. The previous and new behavior should
already be documented on the original issue or PR — link to that rather
than restating it.

If in doubt, default to "land on `main`, ship in the next major."

### Who decides

The release manager for the active release line is the final reviewer of
what goes into the patch release. They coordinate via the release tracking
issue (for example, the [release issue for 50.3.0]). Anyone may propose a
backport by opening a backport PR and linking it from the tracking issue;
inclusion is the release manager's call.

### Active release branches

DataFusion does not maintain Long-Term Support branches. In general only the
most recent `branch-NN` is actively maintained for backports, but if you need
fixes in older releases, we are open to discussion.

Security fixes are an exception: a maintainer may choose to backport a
critical security fix to an older branch even after it would otherwise be
closed. Discuss on the dev list or in a tracking issue before doing so.

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
[datafusion releases tracking issue]: https://github.com/apache/datafusion/issues/19783
[release issue for 50.3.0]: https://github.com/apache/datafusion/issues/18072
[example backport pr]: https://github.com/apache/datafusion/pull/18131
[additional backport pr example]: https://github.com/apache/datafusion/pull/20792
[testing documentation]: testing.md
[api health policy]: api-health.md
[behavior changes]: #behavior-changes
