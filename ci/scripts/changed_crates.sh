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
#   changed-crates <base_ref> [--include-working-tree]
#       Print space-separated list of crate names whose files changed vs base_ref.
#       Only published workspace members (those without `publish = false`) are
#       considered.
#       With `--include-working-tree`, staged, unstaged and untracked
#       (non-ignored) paths are added to the committed range, so uncommitted
#       API edits still select their crate. The hosted workflow does not pass
#       the flag and keeps the committed-range selection.
#
#   semver-check <base_ref> <packages...>
#       Run cargo-semver-checks for the given packages against base_ref.
#       Output and exit code are passed through unchanged; the caller is
#       responsible for capturing/formatting them.

set -euo pipefail

# ── changed-crates ──────────────────────────────────────────────────
cmd_changed_crates() {
  local base_ref="${1:?Usage: changed_crates.sh changed-crates <base_ref> [--include-working-tree]}"
  shift

  local include_working_tree=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --include-working-tree) include_working_tree=1 ;;
      *) echo "Unknown option for changed-crates: $1" >&2; exit 1 ;;
    esac
    shift
  done

  # 1. Files changed between the PR and the base branch.
  local changed_files
  changed_files=$(git diff --name-only "${base_ref}...HEAD")

  if [[ $include_working_tree -eq 1 ]]; then
    # Staged and unstaged changes to tracked files, plus untracked files that
    # are not ignored. `--no-renames` reports a rename as a deletion and an
    # addition, so the crate a file moved out of is selected as well. Deleted
    # paths are listed too, so removing a public item still selects its crate.
    changed_files+=$'\n'$(git diff --name-only --no-renames HEAD)
    changed_files+=$'\n'$(git ls-files --others --exclude-standard)
  fi

  # 2. Every publishable workspace member, one per line as
  #    "<crate-name> <crate-dir>". `publish = false` in Cargo.toml shows
  #    up as `"publish": []` in cargo metadata, so filtering on that
  #    excludes internal crates without a manual exclusion list.
  local crates
  crates=$(cargo metadata --no-deps --format-version 1 | jq -r '
    (.workspace_root + "/") as $root
    | .packages[]
    | select(.publish != [])
    | "\(.name) \(.manifest_path | ltrimstr($root) | rtrimstr("/Cargo.toml"))"
  ')

  # 3. Keep crates whose directory contains a changed file. The test is a
  #    literal prefix comparison, so a directory name is never read as a
  #    regular expression.
  local selected=()
  local name dir path
  while read -r name dir; do
    [[ -n "$name" ]] || continue
    while IFS= read -r path; do
      if [[ "$path" == "${dir}/"* ]]; then
        selected+=("$name")
        break
      fi
    done <<<"$changed_files"
  done <<<"$crates"

  # The guarded expansion keeps `set -u` happy on bash 3.2 (macOS) when
  # nothing was selected.
  echo "${selected[@]+${selected[@]}}"
}

# ── semver-check ────────────────────────────────────────────────────
cmd_semver_check() {
  local base_ref="${1:?Usage: changed_crates.sh semver-check <base_ref> <packages...>}"
  shift

  local args=()
  for pkg in "$@"; do
    args+=(--package "$pkg")
  done

  cargo semver-checks --baseline-rev "$base_ref" "${args[@]}"
}

# ── main ────────────────────────────────────────────────────────────
cmd="${1:?Usage: changed_crates.sh <changed-crates|semver-check> [args...]}"
shift

case "$cmd" in
  changed-crates) cmd_changed_crates "$@" ;;
  semver-check)   cmd_semver_check "$@" ;;
  *) echo "Unknown command: $cmd" >&2; exit 1 ;;
esac
