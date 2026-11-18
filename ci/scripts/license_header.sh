#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
  hawkeye format --config "${HAWKEYE_CONFIG}"
else
  echo "[${SCRIPT_NAME}] \`hawkeye check --config ${HAWKEYE_CONFIG}\`"
  hawkeye check --config "${HAWKEYE_CONFIG}"
fi
