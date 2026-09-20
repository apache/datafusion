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

# Regenerates the configuration and function documentation pages and checks
# that the committed pages match, the same way the "check configs.md and
# ***_functions.md is up-to-date" job does. With `--write`, replaces the pages
# with the generated ones.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${SCRIPT_DIR}/utils/git.sh"

CONFIG_DOCS_DIR="docs/source/user-guide"
FUNCTION_DOCS_DIR="docs/source/user-guide/sql"

MODE="check"
ALLOW_DIRTY=0

usage() {
  cat >&2 <<USAGE
Usage: $0 [--write] [--allow-dirty]

Checks that docs/source/user-guide/configs.md and the aggregate, scalar, and window
function pages under docs/source/user-guide/sql/ match the output of
dev/update_config_docs.sh and dev/update_function_docs.sh.
--write        Replace the pages with the generated ones (requires a clean git worktree, no uncommitted changes).
--allow-dirty  Allow \`--write\` to run even when the git worktree has uncommitted changes.
USAGE
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

cd "${ROOT_DIR}"

if [[ "$MODE" == "write" && $ALLOW_DIRTY -eq 0 ]]; then
  require_clean_work_tree "$SCRIPT_NAME" || exit 1
fi

if ! command -v npx >/dev/null 2>&1; then
  echo "[${SCRIPT_NAME}] npx is required to run the prettier check. Install Node.js (e.g., brew install node) and re-run." >&2
  exit 1
fi

# One scratch directory beneath each documentation directory, so Prettier finds
# the same configuration as for the committed pages.
SCRATCH_DIRS=()
cleanup() {
  rm -rf ${SCRATCH_DIRS[@]+"${SCRATCH_DIRS[@]}"}
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
CONFIG_SCRATCH="$(mktemp -d "${ROOT_DIR}/${CONFIG_DOCS_DIR}/.config-docs-check.XXXXXX")"
SCRATCH_DIRS+=("${CONFIG_SCRATCH}")
FUNCTION_SCRATCH="$(mktemp -d "${ROOT_DIR}/${FUNCTION_DOCS_DIR}/.function-docs-check.XXXXXX")"
SCRATCH_DIRS+=("${FUNCTION_SCRATCH}")

./dev/update_config_docs.sh --output-dir "${CONFIG_SCRATCH}"
./dev/update_function_docs.sh --output-dir "${FUNCTION_SCRATCH}"

GENERATED=(
  "${CONFIG_SCRATCH}/configs.md"
  "${FUNCTION_SCRATCH}/aggregate_functions.md"
  "${FUNCTION_SCRATCH}/scalar_functions.md"
  "${FUNCTION_SCRATCH}/window_functions.md"
)
COMMITTED=(
  "${CONFIG_DOCS_DIR}/configs.md"
  "${FUNCTION_DOCS_DIR}/aggregate_functions.md"
  "${FUNCTION_DOCS_DIR}/scalar_functions.md"
  "${FUNCTION_DOCS_DIR}/window_functions.md"
)

if [[ "$MODE" == "write" ]]; then
  for i in "${!COMMITTED[@]}"; do
    cp "${GENERATED[$i]}" "${COMMITTED[$i]}" || {
      echo "[${SCRIPT_NAME}] failed to copy the generated page to ${COMMITTED[$i]}" >&2
      exit 1
    }
    echo "✅ ${COMMITTED[$i]} updated."
  done
  exit 0
fi

stale_count=0
for i in "${!COMMITTED[@]}"; do
  diff_status=0
  diff -u -L "${COMMITTED[$i]} (committed)" -L "${COMMITTED[$i]} (generated)" \
    "${COMMITTED[$i]}" "${GENERATED[$i]}" > "${GENERATED[$i]}.diff" || diff_status=$?
  case "${diff_status}" in
    0)
      echo "✅ ${COMMITTED[$i]} is up-to-date."
      ;;
    1)
      stale_count=$((stale_count + 1))
      echo ""
      echo "❌ ${COMMITTED[$i]} is out of date."
      echo "------------------------------------------------------------"
      cat "${GENERATED[$i]}.diff"
      echo "------------------------------------------------------------"
      ;;
    *)
      echo "❌ diff exited with status ${diff_status} while comparing ${COMMITTED[$i]}; no comparison result." >&2
      exit "${diff_status}"
      ;;
  esac
done

if [[ ${stale_count} -gt 0 ]]; then
  echo ""
  echo "${stale_count} generated page(s) out of date. To update them, run:"
  echo ""
  echo "  ./ci/scripts/check_config_function_docs.sh --write"
  exit 1
fi
