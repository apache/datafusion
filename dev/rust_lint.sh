#!/bin/bash

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

# This script runs all the Rust lints locally the same way the
# DataFusion CI does
#
# Note: The installed checking tools (e.g., taplo) are not guaranteed to match
# the CI versions for simplicity, there might be some minor differences. Check
# `.github/workflows` for the CI versions.
#
#
#
# For each lint scripts:
#
# By default, they run in check mode:
#     ./ci/scripts/rust_fmt.sh
#
# With `--write`, scripts perform best-effort auto fixes:
#     ./ci/scripts/rust_fmt.sh --write
#
# The `--write` flag assumes a clean git repository (no uncommitted changes); to force
# auto fixes even if there are unstaged changes, use `--allow-dirty`:
#     ./ci/scripts/rust_fmt.sh --write --allow-dirty
#
# New scripts can use `rust_fmt.sh` as a reference.

set -euo pipefail

usage() {
  cat >&2 <<EOF
Usage: $0 [--write] [--allow-dirty]

Runs the local Rust lint suite similar to CI.
--write        Run formatters, clippy and other non-functional checks in best-effort write/fix mode (requires a clean git worktree, no uncommitted changes; some checks are test-only and ignore this flag).
--allow-dirty  Allow \`--write\` to run even when the git worktree has uncommitted changes.
EOF
  exit 1
}

ensure_tool() {
  local cmd="$1"
  local install_cmd="$2"
  if ! command -v "$cmd" &> /dev/null; then
    echo "Installing $cmd using: $install_cmd"
    eval "$install_cmd"
  fi
}

MODE="check"
ALLOW_DIRTY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --write)
      MODE="write"
      ;;
    --allow-dirty)
      ALLOW_DIRTY=1
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
  shift
done

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"

ensure_tool "taplo" "cargo install taplo-cli"
ensure_tool "hawkeye" "cargo install hawkeye --locked"
ensure_tool "typos" "cargo install typos-cli --locked"

run_step() {
  local name="$1"
  shift
  echo "[${SCRIPT_NAME}] Running ${name}"
  "$@"
}

declare -a WRITE_STEPS=(
  "ci/scripts/rust_fmt.sh|true"
  "ci/scripts/rust_clippy.sh|true"
  "ci/scripts/rust_toml_fmt.sh|true"
  "ci/scripts/license_header.sh|true"
  "ci/scripts/typos_check.sh|true"
  "ci/scripts/doc_prettier_check.sh|true"
)

declare -a READONLY_STEPS=(
  "ci/scripts/rust_docs.sh|false"
)

for entry in "${WRITE_STEPS[@]}" "${READONLY_STEPS[@]}"; do
  IFS='|' read -r script_path supports_write <<<"$entry"
  script_name="$(basename "$script_path")"
  args=()
  if [[ "$supports_write" == "true" && "$MODE" == "write" ]]; then
    args+=(--write)
    [[ $ALLOW_DIRTY -eq 1 ]] && args+=(--allow-dirty)
  fi
  if [[ ${#args[@]} -gt 0 ]]; then
    run_step "$script_name" "$script_path" "${args[@]}"
  else
    run_step "$script_name" "$script_path"
  fi
done
