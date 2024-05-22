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

# Introduction

We welcome and encourage contributions of all kinds, from all levels, such as:

1. Tickets with issue reports or feature requests
2. Discussions
3. Documentation improvements
4. Code, both PR and (especially) PR Review.

In addition to submitting new PRs, we have a healthy tradition of community
members reviewing each other's PRs. Doing so is a great way to help the
community as well as get more familiar with Rust and the relevant codebases.

## Finding and Creating Issues to Work On

You can find a curated [good-first-issue] list to help you get started.

DataFusion is an open contribution project, and thus there is no particular
project imposed deadline for completing any issue or any restriction on who can
work on an issue, nor how many people can work on an issue at the same time.

Contributors drive the project forward based on their own priorities and
interests and thus you are free to work on any issue that interests you.

If someone is already working on an issue that you want or need but hasn't
been able to finish it yet, you should feel free to work on it as well. In
general it is both polite and will help avoid unnecessary duplication of work if
you leave a note on an issue when you start working on it.

If you want to work on an issue which is not already assigned to someone else
and there are no comment indicating that someone is already working on that
issue then you can assign the issue to yourself by submitting a single word
comment `take`. This will assign the issue to yourself. However, if you are
unable to make progress you should unassign the issue by using the `unassign me`
link at the top of the issue page (and ask for help if are stuck) so that
someone else can get involved in the work.

If you plan to work on a new feature that doesn't have an existing ticket, it is
a good idea to open a ticket to discuss the feature. Advanced discussion often
helps avoid wasted effort by determining early if the feature is a good fit for
DataFusion before too much time is invested. It also often helps to discuss your
ideas with the community to get feedback on implementation.

[good-first-issue]: https://github.com/apache/datafusion/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22

# Developer's guide

## Pull Request Overview

We welcome pull requests (PRs) from anyone in the community.

DataFusion is a rapidly evolving project and we try to review and merge PRs quickly.

Review bandwidth is currently our most limited resource, and we highly encourage reviews by the broader community. If you are waiting for your PR to be reviewed, consider helping review other PRs that are waiting. Such review both helps the reviewer to learn the codebase and become more expert, as well as helps identify issues in the PR (such as lack of test coverage), that can be addressed and make future reviews faster and more efficient.

The lifecycle of a PR is:

1. Create a PR targeting the `main` branch.
2. For new contributors a committer must first trigger the CI tasks. Please mention the members from committers list in the PR to help trigger the CI
3. Your PR will be reviewed. Please respond to all feedback on the PR: you don't have to change the code, but you should acknowledge the feedback. PRs waiting for the feedback for more than a few days will be marked as draft.
4. Once the PR is approved, one of the [committers] will merge your PR, typically within 24 hours. We leave approved "major" changes (see below) open for 24 hours prior to merging, and sometimes leave "minor" PRs open for the same time to permit additional feedback.

Note that the above time frames are estimates. Due to limited committer
bandwidth, it may take longer to merge your PR. Please wait
patiently. If it has been several days you can friendly ping the
committer who approved your PR to help remind them to merge it.

[committers]: https://people.apache.org/phonebook.html?unix=datafusion

## Creating Pull Requests

We recommend splitting your contributions into multiple smaller focused PRs rather than large PRs (500+ lines) because:

1. The PR is more likely to be reviewed quickly -- our reviewers struggle to find the contiguous time needed to review large PRs.
2. The PR discussions tend to be more focused and less likely to get lost among several different threads.
3. It is often easier to accept and act on feedback when it comes early on in a small change, before a particular approach has been polished too much.

If you are concerned that a larger design will be lost in a string of small PRs, creating a large draft PR that shows how they all work together can help.

Note all commits in a PR are squashed when merged to the `main` branch so there is one commit per PR.

# Reviewing Pull Requests

Some helpful links:

- [PRs Waiting for Review]
- [Approved PRs Waiting for Merge]

[prs waiting for review]: https://github.com/apache/datafusion/pulls?q=is%3Apr+is%3Aopen+-review%3Aapproved+-is%3Adraft+
[approved prs waiting for merge]: https://github.com/apache/datafusion/pulls?q=is%3Apr+is%3Aopen+review%3Aapproved+-is%3Adraft

When reviewing PRs, please remember our primary goal is to improve DataFusion and its community together. PR feedback should be constructive with the aim to help improve the code as well as the understanding of the contributor.

Please ensure any issues you raise contains a rationale and suggested alternative -- it is frustrating to be told "don't do it this way" without any clear reason or alternate provided.

Some things to specifically check:

1. Is the feature or fix covered sufficiently with tests (see `Test Organization` below)?
2. Is the code clear, and fits the style of the existing codebase?

## "Major" and "Minor" PRs

Since we are a worldwide community, we have contributors in many timezones who review and comment. To ensure anyone who wishes has an opportunity to review a PR, our committers try to ensure that at least 24 hours passes between when a "major" PR is approved and when it is merged.

A "major" PR means there is a substantial change in design or a change in the API. Committers apply their best judgment to determine what constitutes a substantial change. A "minor" PR might be merged without a 24 hour delay, again subject to the judgment of the committer. Examples of potential "minor" PRs are:

1. Documentation improvements/additions
2. Small bug fixes
3. Non-controversial build-related changes (clippy, version upgrades etc.)
4. Smaller non-controversial feature additions

The good thing about open code and open development is that any issues in one change can almost always be fixed with a follow on PR.

## Stale PRs

Pull requests will be marked with a `stale` label after 60 days of inactivity and then closed 7 days after that.
Commenting on the PR will remove the `stale` label.

## Specifications

We formalize some DataFusion semantics and behaviors through specification
documents. These specifications are useful to be used as references to help
resolve ambiguities during development or code reviews.

You are also welcome to propose changes to existing specifications or create
new specifications as you see fit.

Here is the list current active specifications:

- [Output field name semantic](https://datafusion.apache.org/contributor-guide/specification/output-field-name-semantic.html)
- [Invariants](https://datafusion.apache.org/contributor-guide/specification/invariants.html)

All specifications are stored in the `docs/source/specification` folder.
