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

# Reviewing Pull Requests

When reviewing PRs, our primary goal is to improve DataFusion and its community
together. PR feedback should be constructive and help improve the code as well
as the understanding of the contributor.

Review bandwidth is currently our most limited resource, and reviews from the
broader community are both welcomed and encouraged. Reviewing PRs is a great way
to learn the codebase, and you do not need to be a committer to leave valuable
review feedback. In fact one of the best ways to become a committer is to
thoughtfully review other PRs.

Please ensure any comments you leave contain a rationale and suggested
alternative -- it is frustrating to be told "don't do it this way" without any
clear reason or alternate provided.

The criteria in this guide are also a useful checklist when preparing your own
PR for review.

## PR Review Mechanics

Some helpful links:

- [PRs Waiting for Review] on GitHub
- [Approved PRs Waiting for Merge] on GitHub

[prs waiting for review]: https://github.com/apache/datafusion/pulls?q=is%3Apr+is%3Aopen+-review%3Aapproved+-is%3Adraft+
[approved prs waiting for merge]: https://github.com/apache/datafusion/pulls?q=is%3Apr+is%3Aopen+review%3Aapproved+-is%3Adraft

The overall PR lifecycle (CI triggering, approval, the 24 hour rule for
"major" PRs, and merging) is described in the
[Pull Request Overview](index.md#pull-request-overview) section of the
contributor guide.

Practical tips:

1. Check out the changes locally to explore them in your IDE or with an
   agent, e.g. `gh pr checkout <PR number>` using the [GitHub CLI].
2. There is normally no need to rerun tests locally that CI has already run.
3. Leave comments on specific lines of the diff where possible, so the
   discussion has context.
4. If you review a PR but don't feel confident approving it, leaving comments
   is still valuable: a partial review (e.g. "I reviewed the tests and they
   look good") helps the next reviewer focus their time.
5. Anything that does not need to block the current PR can be noted as a
   potential follow up (ideally by filing a ticket), keeping the PR focused
   and quick to merge.

[github cli]: https://cli.github.com/

## Review the PR Description

The PR description is often what users and contributors will find when they run
`git log` / `git blame` and ask "why is the code like this?".

Check that the description:

1. Concisely describes the **problem being solved from the user's point of
   view**.

2. Follows the [PR template], and answers the template's questions.

3. Accurately describes what the PR actually does.

4. Explicitly calls out any user-facing or API changes (see
   [Review the Code](#review-the-code) below).

[pr template]: https://github.com/apache/datafusion/blob/main/.github/pull_request_template.md

## Review the Code Comments

Well written code comments are what makes the codebase understandable to the
next contributor.

Check that:

1. The code has adequate comments, and that the comments focus on the
   **rationale** for any non-obvious change (the "why"), not a restatement of
   what the code does (the "what"), which is typically clear from reading the
   code itself.
2. Comments do not narrate irrelevant internal implementation details or the
   history of how the change was developed (this is common in LLM-assisted
   code, e.g. "// changed to use a HashMap" or "// this handles the case
   mentioned above"). Such comments become irrelevant as soon as the PR merges.
3. When comments refer to other structs, functions, or modules, they should use
   [rustdoc intra-doc links] (e.g. `` [`SessionContext`] ``) rather than plain
   text names, so that `cargo doc` link checking ensures the references stay
   valid as the code evolves.
4. New public APIs have doc comments, including examples where appropriate
   (doc examples are also tested by CI, so they double as test coverage).

[rustdoc intra-doc links]: https://doc.rust-lang.org/rustdoc/write-documentation/linking-to-items-by-name.html

## Review the Test Coverage

Check that the feature or fix is covered sufficiently with tests (see the
[Testing](testing.md) guide for more details): the PR should include tests for
any new functionality, and a bug fix should include a test that reproduces the
reported problem.

Guidelines for evaluating tests:

1. Prefer `sqllogictest` (`.slt`) tests or DataFrame API tests where
   possible, as they exercise **user visible behavior** and are less coupled
   to internal implementation details than unit tests.
2. Verify tests cover edge cases and common failure scenarios, not just the 
   common successful path. However, it is NOT necessary to test every possible 
   error path, especially if it is difficult to trigger or unlikely to occur in 
   practice.
3. Verify test coverage of changed code using the `codecov` check on the PR
   or run [`cargo llvm-cov`] locally for an HTML report. Use judgement about
   any uncovered lines (e.g. error paths that are hard to trigger may be
   fine) -- the goal is confidence in the change, not slavishly hitting some
   coverage number.
4. Avoid tests with lots of repeated boilerplate: when many tests share
   near-identical setup, it is hard to understand what is different
   (and thus what is actually being tested) between them. Make the _difference_
   between cases obvious.
5. Check that tests assert on specific expected values or plans (e.g. via
   `insta` snapshots or `.slt` expected output) rather than merely checking
   "no error occurred".

[`cargo llvm-cov`]: https://github.com/taiki-e/cargo-llvm-cov

## Review the Code

Check that:

1. The code is clear and fits the style of the existing codebase.
2. New APIs are consistent with existing public APIs and patterns; where a
   similar mechanism already exists, the PR should extend it rather than
   introduce a parallel one.
3. Any changes to the public API follow the [API health policy].
4. The change is appropriately scoped: unrelated refactoring, formatting
   churn, or drive-by changes make review longer and are better as separate
   PRs.
5. New errors are actionable, mention the offending item, and use
   the right error variant (e.g. `plan_err!` for user-triggerable errors vs
   `internal_err!` for invariant violations).

[api health policy]: api-health.md

## Review the Performance

Performance is a key DataFusion feature. See [Performance Improvements](index.md#performance-improvements)
for the project policy: an improvement should be "enough" to justify any
added code complexity, and performance PRs should come with benchmark
results.

When reviewing:

1. Find any relevant existing benchmarks and run them against `main`:
   the [system level SQL benchmarks] are run with `bench.sh` (see the
   [benchmarks README]), and microbenchmarks (e.g. in
   `datafusion/functions/benches`) are run with `cargo bench`.
2. Be aware that benchmarking on a machine where other
   work is being done will make results hard to reproduce. Prefer a quiet,
   dedicated machine and repeated runs.
3. If the PR claims a performance improvement, check that the reported
   results are reproducible and that the benchmark exercises the changed
   code path.

[system level sql benchmarks]: https://github.com/apache/datafusion/tree/main/benchmarks
[benchmarks readme]: https://github.com/apache/datafusion/blob/main/benchmarks/README.md

## Best Practices for Reviewers

Here are some suggested best practices for reviewers to follow when reviewing PRs.

### Review Tone: Thank Contributors and Praise Good Work Specifically

Open reviews by thanking the author by name, and when a PR is well done, say
specifically what makes it good -- positive feedback encourages people to keep
contributing and helps them understand what is valued in the project.

### State Approval Conditions Explicitly

If you are not ready to approve, list concretely what you would need to see
before approving (e.g. "benchmark results and an upgrade guide entry") so
the author has a clear path to merge.

### Defer Non-Blocking Work to Follow-On Tickets

Explicitly move non-critical suggestions to "follow on PR" status and file
(or ask the author to file) tickets for them, so good PRs merge quickly
without scope creep.

Similarly, when a PR mixes refactoring with behavior changes or fixes a narrow
problem with a broad mechanism, ask for it to be split or scoped down rather
than reviewing it as-is.

### Verify Tests Actually Pin the Bug ("Ablation Testing")

For bug fixes, revert the fix locally and check that the new test fails
without it -- several tests have been found during review that pass even
with the fix reverted.

### Narrate What You Verified When Approving

Rather than a bare "LGTM", say what you actually checked ("traced the state
transitions by hand", "confirmed the hasher change cannot affect ordering")
so the approval carries reviewable evidence.

### Invite Additional Committers on Core Changes

For changes to core, widely-shared code, leave the PR open for other
committers to look at and cc those who know the area, even after you have
approved.
