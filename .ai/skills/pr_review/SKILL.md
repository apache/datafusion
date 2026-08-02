---
name: pr_review
description: Review Apache DataFusion pull requests following the project's PR review guide. Use whenever asked to review a DataFusion PR or PR URL, and whenever creating a PR, to check the changes against the same criteria before submitting.
---

# DataFusion PR Review

This skill describes the mechanics for doing PR reviews from the command line.

When creating a PR, skip the "Collect PR context" step and instead check the
changes against each area of the
[PR review guide](../../../docs/source/contributor-guide/pr_review.md) before
submitting.

## Collect PR context

- Check out the PR locally: `gh pr checkout <PR number>` (ask first if the
  working tree has other work in progress).
- Fetch the PR description, comments, and reviews:
  `gh pr view <PR number> --json title,body,comments,reviews`
- Fetch CI status: `gh pr checks <PR number>`.

## Compute the diff

```bash
git fetch apache main
git diff $(git merge-base HEAD apache/main)
```

## Review checklist

Work through each area from the
[PR review guide](../../../docs/source/contributor-guide/pr_review.md).