#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
CLIPPY_FEATURES="avro,integration-tests,extended_tests"
CLIPPY_ARGS=(--all-targets --workspace)
CLIPPY_LINT_ARGS=(-- -D warnings)

source "${SCRIPT_DIR}/utils/git.sh"

MODE="check"
ALLOW_DIRTY=0

usage() {
  cat >&2 <<EOF
Usage: $SCRIPT_NAME [--write] [--allow-dirty]

Runs \`cargo clippy\` with the standard feature set and with all features.
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
  CLIPPY_CMD+=(--fix --allow-dirty --allow-staged)
fi

run_clippy() {
  local clippy_cmd=("${CLIPPY_CMD[@]}" "${CLIPPY_ARGS[@]}" "$@" "${CLIPPY_LINT_ARGS[@]}")
  echo "[${SCRIPT_NAME}] \`${clippy_cmd[*]}\`"
  "${clippy_cmd[@]}"
}

run_clippy --features "$CLIPPY_FEATURES"
run_clippy --all-features
