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

EXAMPLES_DIR="datafusion-examples/examples"
SKIP_LIST=("ffi")
ONLY_GROUP=""

# ------------------------
# Argument parsing
# ------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --group)
      ONLY_GROUP="${2:-}"
      if [[ -z "$ONLY_GROUP" ]]; then
        echo "Error: --group requires a value"
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--group <group_name>]"
      exit 1
      ;;
  esac
done

# ------------------------
# Helpers
# ------------------------
title_case() {
  # custom_data_source -> Custom Data Source
  echo "$1" \
    | tr '_' ' ' \
    | awk '{ for (i=1; i<=NF; i++) { $i = toupper(substr($i,1,1)) substr($i,2) } print }'
}

skip() {
  local value="$1"
  for item in "${SKIP_LIST[@]}"; do
    [[ "$item" == "$value" ]] && return 0
  done
  return 1
}

generate_group_docs() {
  local group="$1"

  group_title="$(title_case "$group") Examples"

  echo "## ${group_title}"
  echo
  echo "### Group: \`${group}\`"
  echo
  echo "#### Category: Single Process"
  echo
  echo "| Subcommand | File Path | Description |"
  echo "| ---------- | --------- | ----------- |"

  for file in $(find "$EXAMPLES_DIR/$group" -maxdepth 1 -name "*.rs" -exec basename {} \; | sort); do
    [[ "$file" == "main.rs" ]] && continue

    subcommand="${file%.rs}"
    path="examples/${group}/${file}"

    echo "| ${subcommand} | [\`${group}/${file}\`](${path}) | TODO |"
  done

  echo
}

# ------------------------
# Main logic
# ------------------------
if [[ -n "$ONLY_GROUP" ]]; then
  if [[ ! -d "$EXAMPLES_DIR/$ONLY_GROUP" ]]; then
    echo "Error: group '$ONLY_GROUP' does not exist in $EXAMPLES_DIR"
    exit 1
  fi

  if skip "$ONLY_GROUP"; then
    echo "Group '$ONLY_GROUP' is skipped"
    exit 0
  fi

  generate_group_docs "$ONLY_GROUP"
else
  for group in $(find "$EXAMPLES_DIR" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort); do
    skip "$group" && continue
    generate_group_docs "$group"
  done
fi

