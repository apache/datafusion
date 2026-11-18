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

usage() {
  cat >&2 <<EOF
Usage: $0 [--write] [--allow-dirty]

Runs \`cargo fmt --all -- --check\` by default to verify Rust formatting.
--write        Run \`cargo fmt --all\` to auto-fix formatting (requires a clean git worktree, no uncommitted changes).
--allow-dirty  Allow \`--write\` to run even when the git worktree has uncommitted changes.
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
  echo "[${SCRIPT_NAME}] \`cargo fmt --all\`"
  cargo fmt --all
else
  echo "[${SCRIPT_NAME}] \`cargo fmt --all -- --check\`"
  cargo fmt --all -- --check
fi
