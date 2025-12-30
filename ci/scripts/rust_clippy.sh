#!/usr/bin/env bash
#
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
CLIPPY_FEATURES="avro,integration-tests,extended_tests"
CLIPPY_ARGS=(--all-targets --workspace --features "$CLIPPY_FEATURES")
CLIPPY_LINT_ARGS=(-- -D warnings)

source "${SCRIPT_DIR}/utils/git.sh"

MODE="check"
ALLOW_DIRTY=0

usage() {
  cat >&2 <<EOF
Usage: $SCRIPT_NAME [--write] [--allow-dirty]

Runs \`cargo clippy\` to lint.
--write         Run \`cargo clippy --fix\` to apply fixes for clippy lints (requires a clean git worktree, no uncommitted changes).
--allow-dirty   Allow \`--write\` to run even when the git worktree has uncommitted or staged changes.
EOF
  exit 1
}

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

if [[ "$MODE" == "write" && $ALLOW_DIRTY -eq 0 ]]; then
  require_clean_work_tree "$SCRIPT_NAME" || exit 1
fi

if [[ "$MODE" == "write" ]]; then
  ALLOW_DIRTY_ARGS=()
  if [[ $ALLOW_DIRTY -eq 1 ]]; then
    ALLOW_DIRTY_ARGS+=(--allow-dirty --allow-staged)
  fi
  echo "[${SCRIPT_NAME}] \`cargo clippy --fix --all-targets --workspace --features ${CLIPPY_FEATURES} -- -D warnings\`"
  cargo clippy --fix "${ALLOW_DIRTY_ARGS[@]}" "${CLIPPY_ARGS[@]}" "${CLIPPY_LINT_ARGS[@]}"
else
  echo "[${SCRIPT_NAME}] \`cargo clippy --all-targets --workspace --features ${CLIPPY_FEATURES} -- -D warnings\`"
  cargo clippy "${CLIPPY_ARGS[@]}" "${CLIPPY_LINT_ARGS[@]}"
fi
