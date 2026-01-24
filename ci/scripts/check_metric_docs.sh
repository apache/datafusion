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
# software distributed under this License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"

source "${SCRIPT_DIR}/utils/git.sh"

MODE="check"
ALLOW_DIRTY=0

usage() {
  cat >&2 <<EOF
Usage: $SCRIPT_NAME [--write] [--allow-dirty]

Checks if metrics.md is up-to-date by running update_metric_docs.sh.
--write         Run \`./dev/update_metric_docs.sh\` to update metrics.md (requires a clean git worktree, no uncommitted changes).
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

# Ensure we're in the project root
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

METRICS_FILE="docs/source/user-guide/metrics.md"

if [ ! -f "$METRICS_FILE" ]; then
    echo "Warning: $METRICS_FILE not found, skipping check." >&2
    exit 0
fi

if [[ "$MODE" == "write" ]]; then
  echo "[${SCRIPT_NAME}] Running \`./dev/update_metric_docs.sh\` to update metrics.md"
  ./dev/update_metric_docs.sh
else
  echo "[${SCRIPT_NAME}] Checking if metrics.md is up-to-date"
  METRICS_BACKUP=$(mktemp)
  
  # Backup the current file
  cp "$METRICS_FILE" "$METRICS_BACKUP"
  
  # Run the update script (this will modify the file)
  # Suppress output but capture exit code
  if ! ./dev/update_metric_docs.sh > /dev/null 2>&1; then
      echo "Error: Failed to run update_metric_docs.sh. Check permissions and try again." >&2
      # Restore the original file
      mv "$METRICS_BACKUP" "$METRICS_FILE"
      exit 1
  fi
  
  # Compare the updated file with the backup
  if ! diff -q "$METRICS_BACKUP" "$METRICS_FILE" > /dev/null; then
      echo "Error: metrics.md is not up-to-date. Run './dev/update_metric_docs.sh' and commit the changes." >&2
      # Restore the original file
      mv "$METRICS_BACKUP" "$METRICS_FILE"
      exit 1
  fi

  # Clean up the backup file
  rm -f "$METRICS_BACKUP"
fi

