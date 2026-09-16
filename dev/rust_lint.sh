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

# This script runs all the Rust lints locally the same way the
# DataFusion CI does
#
# Note: The installed checking tools (e.g., taplo) are not guaranteed to match
# the CI versions for simplicity, there might be some minor differences. Check
# `.github/workflows` for the CI versions. When this script installs a missing
# tool that has a pinned version in `ci/scripts/utils/tool_versions.sh`, it
# installs that pinned version. An already installed tool is used as is.
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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# `ci/scripts/check_asf_yaml_status_checks.py` runs with `python3` from PATH
# and imports PyYAML. Report a missing prerequisite before any tool is
# installed or any formatter runs, and point at `uv run`, which sets up the
# Python dependencies from the uv workspace.
ensure_python_with_yaml() {
  if ! command -v python3 &> /dev/null; then
    echo "[${SCRIPT_NAME}] python3 was not found on PATH. Please run the suite through uv, which provides Python and its packages: uv run ./dev/rust_lint.sh" >&2
    exit 1
  fi
  if ! python3 -c 'import yaml' &> /dev/null; then
    echo "[${SCRIPT_NAME}] PyYAML is not installed for $(command -v python3). Please run the suite through uv, which installs it: uv run ./dev/rust_lint.sh" >&2
    exit 1
  fi
}

ensure_python_with_yaml

# Load the tool versions shared with CI (for example, LYCHEE_VERSION).
source "${SCRIPT_DIR}/../ci/scripts/utils/tool_versions.sh"

ensure_tool "taplo" "cargo install taplo-cli --locked"
ensure_tool "hawkeye" "cargo install hawkeye --locked"
ensure_tool "typos" "cargo install typos-cli --locked"
ensure_tool "lychee" "cargo install lychee --locked --version ${LYCHEE_VERSION}"

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
  "ci/scripts/check_no_cargo_install_in_workflows.sh|false"
  "ci/scripts/check_asf_yaml_status_checks.py|false"
  "ci/scripts/markdown_link_check.sh|false"
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
