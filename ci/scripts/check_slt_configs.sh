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

# Checks sqllogictest (.slt) files for dangling DataFusion configuration
# settings.
#
# Any configuration set with:
#     set datafusion.<config> = true
# must be reset in the same file with:
#     set datafusion.<config> = false
#
# This prevents configuration state from leaking into subsequent tests,
# which can cause nondeterministic or hard-to-debug failures.
#
# Usage:
#   Check all .slt files under datafusion/sqllogictest/test_files:
#       ./check_slt_configs.sh
#
#   Check a specific file (repo-relative or absolute path):
#       ./check_slt_configs.sh datafusion/sqllogictest/test_files/foo.slt
#       ./check_slt_configs.sh /absolute/path/to/foo.slt
#
# Exit codes:
#   0  All checks passed
#   1  One or more files contain configs set to true but never reset

set -euo pipefail

# Resolve repository root
ROOT_DIR="$(git rev-parse --show-toplevel)"
TARGET_DIR="${ROOT_DIR}/datafusion/sqllogictest/test_files"

FAILED=0

resolve_file() {
  local input="$1"

  # If absolute and exists
  if [[ -f "$input" ]]; then
    realpath "$input"
    return
  fi

  # If relative to repo root
  if [[ -f "${ROOT_DIR}/${input}" ]]; then
    realpath "${ROOT_DIR}/${input}"
    return
  fi

  echo ""
}

# If file argument is passed, check only that file
if [[ $# -eq 1 ]]; then
  RESOLVED_FILE="$(resolve_file "$1")"

  if [[ -z "$RESOLVED_FILE" ]]; then
    echo "❌ File does not exist: $1"
    exit 1
  fi

  FILES="$RESOLVED_FILE"
else
  FILES=$(find "$TARGET_DIR" -type f -name "*.slt")
fi

# Iterate safely even if filenames contain spaces
while IFS= read -r file; do
  echo "Checking file: $file"

  if [[ ! -f "$file" ]]; then
    echo "  ❌ File not found: $file"
    FAILED=1
    continue
  fi

  matches=$(grep -En \
    'set[[:space:]]+datafusion\.[a-zA-Z0-9_.]+[[:space:]]*=[[:space:]]*true' \
    "$file" || true)

  [[ -z "$matches" ]] && continue

  # Process each match line-by-line
  while IFS= read -r match; do
    line_number=$(echo "$match" | cut -d: -f1)
    line_content=$(echo "$match" | cut -d: -f2-)

    # Extract config name
    config=$(echo "$line_content" \
      | sed -E 's/set[[:space:]]+(datafusion\.[a-zA-Z0-9_.]+).*/\1/')

    # Check if reset exists anywhere in file
    if ! grep -Eq \
      "set[[:space:]]+$config[[:space:]]*=[[:space:]]*false" \
      "$file"; then
      echo "  ❌ $config set to true at line $line_number but never reset to false"
      FAILED=1
    fi

  done <<< "$matches"

done <<< "$FILES"

if [[ $FAILED -eq 1 ]]; then
  echo "Config reset check failed."
  exit 1
else
  echo "All SLT config checks passed."
fi
