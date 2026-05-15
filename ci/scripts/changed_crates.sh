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
#       Only published workspace members (those without `publish = false`) are
#       considered.
#
#   latest-release-tag
#       Print the latest stable release tag. RC and other pre-release tags are
#       ignored. Tags must be plain semver values like `53.1.0`.
#
#   semver-check <baseline_ref> <packages...>
#       Run cargo-semver-checks for the given packages against baseline_ref.
#       baseline_ref can be a tag or any git ref. Output and exit code are
#       passed through unchanged; the caller is responsible for capturing and
#       formatting them.

set -euo pipefail

# ── changed-crates ──────────────────────────────────────────────────
cmd_changed_crates() {
  local base_ref="${1:?Usage: changed_crates.sh changed-crates <base_ref>}"

  # 1. Files changed between the PR and the base branch.
  local changed_files
  changed_files=$(git diff --name-only "${base_ref}...HEAD")

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

  # 3. Keep crates whose directory contains a changed file.
  while read -r name dir; do
    if grep -q "^${dir}/" <<<"$changed_files"; then
      echo "$name"
    fi
  done <<<"$crates" | xargs
}

# ── latest-release-tag ──────────────────────────────────────────────
cmd_latest_release_tag() {
  local latest_release_tag
  latest_release_tag=$(git tag --list \
    | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' \
    | sort -V \
    | tail -n1 || true)

  if [ -z "$latest_release_tag" ]; then
    echo "No stable release tags found" >&2
    return 1
  fi

  echo "$latest_release_tag"
}

# ── semver-check ────────────────────────────────────────────────────
cmd_semver_check() {
  local base_ref="${1:?Usage: changed_crates.sh semver-check <baseline_ref> <packages...>}"
  shift

  local args=()
  for pkg in "$@"; do
    args+=(--package "$pkg")
  done

  cargo semver-checks --baseline-rev "$base_ref" "${args[@]}"
}

# ── main ────────────────────────────────────────────────────────────
cmd="${1:?Usage: changed_crates.sh <changed-crates|latest-release-tag|semver-check> [args...]}"
shift

case "$cmd" in
  changed-crates)      cmd_changed_crates "$@" ;;
  latest-release-tag)  cmd_latest_release_tag "$@" ;;
  semver-check)        cmd_semver_check "$@" ;;
  *) echo "Unknown command: $cmd" >&2; exit 1 ;;
esac
