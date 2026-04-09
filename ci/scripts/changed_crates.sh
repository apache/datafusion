#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Helper script for the breaking-changes-detector workflow.
#
# Subcommands:
#   changed-crates <base_ref>
#       Print space-separated list of crate names whose files changed vs base_ref.
#
#   semver-check <base_ref> <packages...>
#       Run cargo-semver-checks for the given packages against base_ref.
#       Prints the (ANSI-stripped) log output to stdout.
#       Exit code matches cargo-semver-checks (0 = pass, non-zero = breaking).
#
#   comment <repo> <pr_number> <check_result> [logs]
#       Upsert or delete a sticky PR comment based on check_result.
#       check_result: "success" deletes any existing comment,
#                     anything else upserts the comment with the provided logs.
#       Requires GH_TOKEN to be set.

set -euo pipefail

MARKER="<!-- semver-check-comment -->"

# ── changed-crates ──────────────────────────────────────────────────
cmd_changed_crates() {
  local base_ref="${1:?Usage: changed_crates.sh changed-crates <base_ref>}"

  # Parse workspace members from root Cargo.toml, excluding internal crates
  # that are not published / not part of the public API.
  local members
  members=$(sed -n '/^members = \[/,/\]/p' Cargo.toml | grep '"' | sed 's/.*"\(.*\)".*/\1/' \
    | grep -v -e '^benchmarks$' -e '^test-utils$' -e '^datafusion/sqllogictest$' -e '^datafusion/doc$')

  local changed_files
  changed_files=$(git diff --name-only "${base_ref}...HEAD")

  local packages=""
  for member in $members; do
    if echo "$changed_files" | grep -q "^${member}/"; then
      local pkg
      pkg=$(grep '^name\s*=' "$member/Cargo.toml" | head -1 | sed 's/.*=\s*"\(.*\)"/\1/')
      if [ -n "$pkg" ]; then
        packages="$packages $pkg"
      fi
    fi
  done

  echo "$packages" | xargs
}

# ── semver-check ────────────────────────────────────────────────────
cmd_semver_check() {
  local base_ref="${1:?Usage: changed_crates.sh semver-check <base_ref> <packages...>}"
  shift

  local args=""
  for pkg in "$@"; do
    args="$args --package $pkg"
  done

  set +e
  # Compare the PR's code against the base branch to detect breaking changes.
  # Use tee to show output in the Actions log while also capturing it.
  cargo semver-checks --baseline-rev "$base_ref" $args 2>&1 | tee /tmp/semver-output.txt
  local exit_code=${PIPESTATUS[0]}
  set -e

  # Strip ANSI escape codes from the captured output for the PR comment.
  sed 's/\x1b\[[0-9;]*m//g' /tmp/semver-output.txt
  return "$exit_code"
}

# ── comment ─────────────────────────────────────────────────────────
cmd_comment() {
  local repo="${1:?Usage: changed_crates.sh comment <repo> <pr_number> <check_result> [logs]}"
  local pr_number="${2:?}"
  local check_result="${3:?}"
  local logs="${4:-}"

  # Find existing comment with our marker
  local comment_id
  comment_id=$(gh api "repos/${repo}/issues/${pr_number}/comments" \
    --jq ".[] | select(.body | contains(\"${MARKER}\")) | .id" | head -1)

  if [ "$check_result" = "success" ]; then
    # Delete the comment if one exists
    if [ -n "$comment_id" ]; then
      gh api "repos/${repo}/issues/comments/${comment_id}" --method DELETE
    fi
  else
    local body="${MARKER}
Thank you for opening this pull request!

Reviewer note: [cargo-semver-checks](https://github.com/obi1kenobi/cargo-semver-checks) reported the current version number is not SemVer-compatible with the changes made since the last release.

Details:

\`\`\`
${logs}
\`\`\`"

    if [ -n "$comment_id" ]; then
      gh api "repos/${repo}/issues/comments/${comment_id}" \
        --method PATCH --field body="$body"
    else
      gh api "repos/${repo}/issues/${pr_number}/comments" \
        --method POST --field body="$body"
    fi
  fi
}

# ── main ────────────────────────────────────────────────────────────
cmd="${1:?Usage: changed_crates.sh <changed-crates|semver-check|comment> [args...]}"
shift

case "$cmd" in
  changed-crates) cmd_changed_crates "$@" ;;
  semver-check)   cmd_semver_check "$@" ;;
  comment)        cmd_comment "$@" ;;
  *) echo "Unknown command: $cmd" >&2; exit 1 ;;
esac
