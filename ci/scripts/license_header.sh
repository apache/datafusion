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

source "${SCRIPT_DIR}/utils/git.sh"

MODE="check"
ALLOW_DIRTY=0
HAWKEYE_CONFIG="licenserc.toml"

usage() {
  cat >&2 <<EOF
Usage: $SCRIPT_NAME [--write] [--allow-dirty]

Checks Apache license headers with \`hawkeye check --config $HAWKEYE_CONFIG\`.
--write         Run \`hawkeye format --config $HAWKEYE_CONFIG\` to auto-add/fix headers (requires a clean git worktree, no uncommitted changes).
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

if [[ "$MODE" == "write" ]]; then
  echo "[${SCRIPT_NAME}] \`hawkeye format --config ${HAWKEYE_CONFIG}\`"
  if ! hawkeye format --config "${HAWKEYE_CONFIG}"; then
    status=$?
    # hawkeye returns exit code 1 when it applies fixes; treat that as success.
    if [[ $status -eq 1 ]]; then
      echo "[${SCRIPT_NAME}] hawkeye format applied fixes (exit 1 treated as success)"
    else
      exit $status
    fi
  fi
else
  echo "[${SCRIPT_NAME}] \`hawkeye check --config ${HAWKEYE_CONFIG}\`"
  hawkeye check --config "${HAWKEYE_CONFIG}"
fi
