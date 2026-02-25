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
# must be restored in the same file using either:
#     set datafusion.<config> = false
# or:
#     reset datafusion.<config> 
# 
# This prevents configuration state from leaking into subsequent tests,
# which can cause nondeterministic or hard-to-debug failures.
# 
# TODO:
# This script currently detects only boolean configurations that are enabled
# via `set datafusion.<config> = true`.
#
# It does NOT detect non-boolean configuration mutations such as:
#     SET datafusion.catalog.default_catalog = 'foo';
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

ROOT_DIR="$(git rev-parse --show-toplevel)"
TARGET_DIR="${ROOT_DIR}/datafusion/sqllogictest/test_files"

FAILED=0

resolve_file() {
  local input="$1"

  if [[ -f "$input" ]]; then
    RESOLVED_FILE="$(realpath "$input")"
    return 0
  fi

  if [[ -f "${ROOT_DIR}/${input}" ]]; then
    RESOLVED_FILE="$(realpath "${ROOT_DIR}/${input}")"
    return 0
  fi

  echo "❌ File does not exist: $input"
  return 1
}

if [[ $# -eq 1 ]]; then
  if ! resolve_file "$1"; then
    exit 1
  fi
  FILES="$RESOLVED_FILE"
else
  FILES=$(find "$TARGET_DIR" -type f -name "*.slt")
fi

while IFS= read -r file; do
  echo "Checking file: $file"

  [[ ! -f "$file" ]] && continue

  # Detect: set datafusion.xxx = true (case-insensitive)
  matches=$(grep -Eni \
    'set[[:space:]]+datafusion\.[a-zA-Z0-9_.]+[[:space:]]*=[[:space:]]*true' \
    "$file" || true)

  [[ -z "$matches" ]] && continue

  while IFS= read -r match; do
    line_number=${match%%:*}
    line_content=${match#*:}

    config=$(echo "$line_content" \
      | sed -E 's/[sS][eE][tT][[:space:]]+(datafusion\.[a-zA-Z0-9_.]+).*/\1/')

    # Check for either (case-insensitive):
    #   set datafusion.xxx = false
    #   reset datafusion.xxx
    if ! grep -Eqi \
      "(set[[:space:]]+${config}[[:space:]]*=[[:space:]]*false)|(reset[[:space:]]+${config})" \
      "$file"; then
      echo "  ❌ ${config} set to true at line $line_number but never reset (false or RESET)"
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
