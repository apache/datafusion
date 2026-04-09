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

CURRENT=$(pwd)

cd "$SCRIPT_DIR/../.." || exit 1

set +e

# Fetch one more commit so HEAD~1 is available (CI uses fetch-depth: 1)
git fetch --deepen=1 2>/dev/null || true
BASELINE_REV=$(git rev-parse HEAD~1)
echo "Baseline revision: $BASELINE_REV"

# Get workspace members from root Cargo.toml, excluding non-public crates
MEMBERS=$(sed -n '/^members = \[/,/\]/p' Cargo.toml | grep '"' | sed 's/.*"\(.*\)".*/\1/' \
  | grep -v -e '^benchmarks$' -e '^test-utils$' -e '^datafusion/sqllogictest$' -e '^datafusion/doc$')

# Get changed files compared to baseline
CHANGED_FILES=$(git diff --name-only "$BASELINE_REV"..HEAD)

# Check which workspace members have changes
PACKAGES=""
for member in $MEMBERS; do
  if echo "$CHANGED_FILES" | grep -q "^${member}/"; then
    pkg=$(grep '^name\s*=' "$member/Cargo.toml" | head -1 | sed 's/.*=\s*"\(.*\)"/\1/')
    if [ -n "$pkg" ]; then
      PACKAGES="$PACKAGES $pkg"
    fi
  fi
done
PACKAGES=$(echo "$PACKAGES" | xargs)
echo "Changed crates: $PACKAGES"

if [ -n "$PACKAGES" ]; then
  echo "Installing cargo-semver-checks..."
  cargo install cargo-semver-checks

  ARGS=""
  for pkg in $PACKAGES; do
    ARGS="$ARGS --package $pkg"
  done

  echo "Running cargo-semver-checks against origin/main..."
  cargo semver-checks --baseline-rev "$BASELINE_REV" $ARGS
else
  echo "No public crates changed, skipping semver checks."
fi


echo "Running cargo clippy..."

cd ${CURRENT} || exit 1

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

CLIPPY_CMD=(cargo clippy)
if [[ "$MODE" == "write" ]]; then
  CLIPPY_CMD+=(--fix)
  if [[ $ALLOW_DIRTY -eq 1 ]]; then
    CLIPPY_CMD+=(--allow-dirty --allow-staged)
  fi
fi
CLIPPY_CMD+=("${CLIPPY_ARGS[@]}" "${CLIPPY_LINT_ARGS[@]}")

echo "[${SCRIPT_NAME}] \`${CLIPPY_CMD[*]}\`"
"${CLIPPY_CMD[@]}"

