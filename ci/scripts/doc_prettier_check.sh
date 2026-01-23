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
PRETTIER_VERSION="2.7.1"
PRETTIER_TARGETS=(
  '{datafusion,datafusion-cli,datafusion-examples,dev,docs}/**/*.md'
  '!datafusion/CHANGELOG.md'
  README.md
  CONTRIBUTING.md
)

source "${SCRIPT_DIR}/utils/git.sh"

MODE="check"
ALLOW_DIRTY=0

usage() {
  cat >&2 <<EOF
Usage: $SCRIPT_NAME [--write] [--allow-dirty]

Runs prettier@${PRETTIER_VERSION} over markdown docs.
--write         Run with \`--write\` to format files (requires a clean git worktree, no uncommitted changes).
--allow-dirty   Allow \`--write\` to run even when the git worktree has uncommitted changes.
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

echo "[${SCRIPT_NAME}] prettier@${PRETTIER_VERSION} ${MODE}"

# Ensure `npx` is available
if ! command -v npx >/dev/null 2>&1; then
  echo "npx is required to run the prettier check. Install Node.js (e.g., brew install node) and re-run." >&2
  exit 1
fi

PRETTIER_MODE=(--check)
if [[ "$MODE" == "write" ]]; then
  PRETTIER_MODE=(--write)
fi

# Ignore subproject CHANGELOG.md because it is machine generated
npx "prettier@${PRETTIER_VERSION}" "${PRETTIER_MODE[@]}" "${PRETTIER_TARGETS[@]}"
