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

# Generates the examples README with the Rust generator and checks that the
# committed `datafusion-examples/README.md` matches it, the same way the
# "check example README is up-to-date" job does. With `--write`, replaces the
# README with the generated one.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${SCRIPT_DIR}/utils/tool_versions.sh"
source "${SCRIPT_DIR}/utils/git.sh"

EXAMPLES_DIR="${ROOT_DIR}/datafusion-examples"
README="${EXAMPLES_DIR}/README.md"

MODE="check"
ALLOW_DIRTY=0

usage() {
  cat >&2 <<USAGE
Usage: $0 [--write] [--allow-dirty]

Checks that datafusion-examples/README.md matches the output of the examples-docs generator.
--write        Replace the README with the generated one (requires a clean git worktree, no uncommitted changes).
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

if [[ "$MODE" == "write" && $ALLOW_DIRTY -eq 0 ]]; then
  require_clean_work_tree "$SCRIPT_NAME" || exit 1
fi

if ! command -v npx >/dev/null 2>&1; then
  echo "[${SCRIPT_NAME}] npx is required to run the prettier check. Install Node.js (e.g., brew install node) and re-run." >&2
  exit 1
fi

# The scratch directory sits beneath the examples directory so Prettier finds
# the same configuration as for the committed README.
SCRATCH_DIR="$(mktemp -d "${EXAMPLES_DIR}/.examples-docs-check.XXXXXX")"
trap 'rm -rf "${SCRATCH_DIR}"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
README_NEW="${SCRATCH_DIR}/README.md"
README_DIFF="${SCRATCH_DIR}/README.diff"

cd "${ROOT_DIR}"

echo "▶ Generating examples README (Rust generator)…"
cargo run --quiet \
  --manifest-path "${EXAMPLES_DIR}/Cargo.toml" \
  --bin examples-docs \
  > "${README_NEW}"

echo "▶ Formatting generated README with prettier ${PRETTIER_VERSION}…"
npx "prettier@${PRETTIER_VERSION}" \
  --parser markdown \
  --write "${README_NEW}"

if [[ "$MODE" == "write" ]]; then
  cp "${README_NEW}" "${README}"
  echo "✅ Examples README updated."
  exit 0
fi

echo "▶ Comparing generated README with committed version…"
diff_status=0
diff -u "${README}" "${README_NEW}" > "${README_DIFF}" || diff_status=$?

case "${diff_status}" in
  0)
    echo "✅ Examples README is up-to-date."
    ;;
  1)
    echo ""
    echo "❌ Examples README is out of date."
    echo ""
    echo "The examples documentation is generated automatically from:"
    echo "  - datafusion-examples/examples/<group>/main.rs"
    echo ""
    echo "To update the README, run:"
    echo ""
    echo "  ./ci/scripts/check_examples_docs.sh --write"
    echo ""
    echo "Diff:"
    echo "------------------------------------------------------------"
    cat "${README_DIFF}"
    echo "------------------------------------------------------------"
    exit 1
    ;;
  *)
    echo "❌ diff exited with status ${diff_status} while comparing the README; no comparison result." >&2
    exit "${diff_status}"
    ;;
esac
