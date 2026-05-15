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

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
CHANGED_CRATES_SH="$SCRIPT_DIR/changed_crates.sh"
TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT

setup_git_repo() {
  local repo_dir=$1
  git -C "$repo_dir" init --quiet
  git -C "$repo_dir" config user.email test@example.com
  git -C "$repo_dir" config user.name test
  git -C "$repo_dir" commit --quiet --allow-empty -m init
}

new_git_repo() {
  local repo_dir
  repo_dir=$(mktemp -d "$TMP_ROOT/repo.XXXXXX")
  setup_git_repo "$repo_dir"
  echo "$repo_dir"
}

run_latest_release_tag() {
  local repo_dir=$1
  (cd "$repo_dir" && "$CHANGED_CRATES_SH" latest-release-tag)
}

assert_eq() {
  local expected=$1
  local actual=$2
  local message=$3
  if [ "$actual" != "$expected" ]; then
    echo "FAIL: $message" >&2
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    exit 1
  fi
}

tag_repo() {
  local repo_dir=$1
  shift

  for tag in "$@"; do
    git -C "$repo_dir" tag "$tag"
  done
}

assert_latest_release_tag() {
  local test_name=$1
  local expected=$2
  shift 2

  local repo_dir
  repo_dir=$(new_git_repo)
  tag_repo "$repo_dir" "$@"

  local actual
  actual=$(run_latest_release_tag "$repo_dir")
  assert_eq "$expected" "$actual" "$test_name"
}

assert_latest_release_tag_fails() {
  local test_name=$1
  shift

  local repo_dir
  repo_dir=$(new_git_repo)
  tag_repo "$repo_dir" "$@"

  if run_latest_release_tag "$repo_dir" >"$TMP_ROOT/out" 2>"$TMP_ROOT/err"; then
    echo "FAIL: $test_name" >&2
    exit 1
  fi
  assert_eq "No stable release tags found" "$(cat "$TMP_ROOT/err")" "$test_name"
}

assert_latest_release_tag "stable tag wins over newer RC" \
  "53.1.0" \
  "53.0.0" "53.1.0-rc1" "53.1.0" "54.0.0-rc1"

assert_latest_release_tag "semver sort handles double-digit versions" \
  "10.0.0" \
  "9.9.9" "10.0.0" "10.0.1-rc1"

assert_latest_release_tag "malformed and namespaced tags are ignored" \
  "2.0.0" \
  "ballista-9.0.0" "python-99.0.0" "2.0" "2.0.0" "3.0.0-alpha1"

assert_latest_release_tag_fails "no tags error"

assert_latest_release_tag_fails "only RC tags error" \
  "53.1.0-rc1" "54.0.0-rc1"

echo "changed_crates.sh tests passed"
