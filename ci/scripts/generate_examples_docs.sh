#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.
#
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance
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
# 
# ==============================================================================
# generate_examples_docs.sh
# ==============================================================================
#
# PURPOSE
# -------
# This script generates the "DataFusion Examples" README section by combining:
#
#   1. The examples that exist on disk:
#        datafusion-examples/examples/<group>/*.rs
#
#   2. The metadata defined in:
#        datafusion-examples/examples.toml
#
# The generated output is written to stdout and is intended to fully replace
# the auto-generated examples section of the README.
#
#
# HOW DOCUMENTATION STAYS IN SYNC
# -------------------------------
#
# • Source of truth for example *files*:
#     The filesystem under datafusion-examples/examples/
#
# • Source of truth for example *metadata* (subcommand + description):
#     datafusion-examples/examples.toml
#
# • This script enforces the following rules:
#
#   - Every example listed in examples.toml MUST exist on disk
#       → otherwise the script fails (hard error)
#
#   - Example files that exist on disk but are NOT in examples.toml
#       → are still documented, but get:
#           subcommand = filename
#           description = TODO
#
#   - CI compares the generated README with the committed version
#       → missing or outdated metadata causes CI to fail
#
# This ensures that:
#   • examples cannot silently disappear
#   • new examples must be documented
#   • documentation always reflects the real code
#
#
# STRUCTURE OF THE OUTPUT
# -----------------------
#
# For each example group:
#
#   ## <Group Name> Examples
#   ### Group: `<group>`
#   #### Category: Single Process | Distributed
#
#   | Subcommand | File Path | Description |
#
# Subcommands come from examples.toml when present.
#
#
# OPTIONAL USAGE
# --------------
#
#   Generate docs for a single group only:
#
#     ./generate_examples_docs.sh --group dataframe
#
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

# Root of the examples crate
EXAMPLES_ROOT="datafusion-examples"

# Directory that contains example groups
EXAMPLES_DIR="$EXAMPLES_ROOT/examples"

# TOML file with example metadata (subcommand + description)
DESCRIPTIONS_FILE="$EXAMPLES_ROOT/examples.toml"

# Groups that intentionally do not follow the standard layout
SKIP_LIST=("ffi")

# Groups that run in distributed mode
# Used only for documentation categorization
DISTRIBUTED_GROUPS=("flight")

# Default execution category for groups
DEFAULT_CATEGORY="Single Process"

# Optional CLI filter: generate docs for only one group
ONLY_GROUP=""

# ------------------------------------------------------------------------------
# Argument parsing 
# ------------------------------------------------------------------------------

# Supports:
#   --group <group_name>
#
# Used mainly for local testing and iteration
while [[ $# -gt 0 ]]; do
  case "$1" in
    --group)
      ONLY_GROUP="${2:-}"
      if [[ -z "$ONLY_GROUP" ]]; then
        echo "Error: --group requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--group <group_name>]" >&2
      exit 1
      ;;
  esac
done

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

# Returns success if the given group should be skipped
skip() {
  local value="$1"
  for item in "${SKIP_LIST[@]}"; do
    [[ "$item" == "$value" ]] && return 0
  done
  return 1
}

# Converts snake_case to Title Case
#   custom_data_source -> Custom Data Source
title_case() {
  echo "$1" \
    | tr '_' ' ' \
    | awk '{ for (i=1; i<=NF; i++) $i = toupper(substr($i,1,1)) substr($i,2); print }'
}

# Returns success if the group is considered "Distributed"
is_distributed() {
  local value="$1"
  for item in "${DISTRIBUTED_GROUPS[@]}"; do
    [[ "$item" == "$value" ]] && return 0
  done
  return 1
}

# ------------------------------------------------------------------------------
# TOML parsing helpers
# ------------------------------------------------------------------------------

# Reads examples.toml and emits all examples for a group
#
# Output format (one per line):
#   subcommand|file|description
#
# Used only for consistency checks
get_group_examples() {
  local group="$1"

  awk -v group="$group" '
    /^\[.*\]/ { in_group = ($0 == "[" group "]"); next }
    in_group && $0 ~ /\{[[:space:]]*file[[:space:]]*=/ {
      key=$1
      sub(/[[:space:]]*=.*/, "", key)

      file=$0
      sub(/.*file[[:space:]]*=[[:space:]]*"/,"",file)
      sub(/".*/,"",file)

      desc=$0
      sub(/.*desc[[:space:]]*=[[:space:]]*"/,"",desc)
      sub(/".*/,"",desc)

      print key "|" file "|" desc
    }
  ' "$DESCRIPTIONS_FILE"
}

# Given a group and filename, extract metadata from examples.toml
#
# Output format:
#   subcommand|description
#
# If no entry exists:
#   |TODO
#
# This allows new examples to appear in docs without breaking generation
get_metadata_by_file() {
  local group="$1"
  local file="$2"

  awk -v group="$group" -v file="$file" '
    /^\[.*\]/ { in_group = ($0 == "[" group "]"); next }
    in_group && $0 ~ "file[[:space:]]*=[[:space:]]*\"" file "\"" {
      subcmd=$1
      sub(/[[:space:]]*=.*/, "", subcmd)

      desc=$0
      sub(/.*desc[[:space:]]*=[[:space:]]*"/,"",desc)
      sub(/".*/,"",desc)

      print subcmd "|" desc
      found=1
      exit
    }
    END {
      if (!found) print "" "|" "TODO"
    }
  ' "$DESCRIPTIONS_FILE"
}

# ------------------------------------------------------------------------------
# Consistency checks
# ------------------------------------------------------------------------------

# Ensures examples.toml does not reference non-existent files
#
# This is a hard error because it means documentation is stale or incorrect
check_group_consistency() {
  local group="$1"
  local dir="$EXAMPLES_DIR/$group"

  FILES_ON_DISK="$(find "$dir" -maxdepth 1 -name "*.rs" ! -name "main.rs" -exec basename {} \; | sort)"
  FILES_IN_TOML="$(get_group_examples "$group" | cut -d'|' -f2 | sort)"

  # TOML → disk must exist (hard error)
  echo "$FILES_IN_TOML" | while read -r file; do
    [[ -z "$file" ]] && continue
    if ! echo "$FILES_ON_DISK" | grep -qx "$file"; then
      echo "ERROR: examples.toml lists file '$file' in group '$group' but it does not exist on disk" >&2
      exit 1
    fi
  done
}

# ------------------------------------------------------------------------------
# Static README header 
# ------------------------------------------------------------------------------

# Everything up to this point is static documentation content.
# The auto-generated examples section starts after this.
cat <<'EOF'
<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion Examples

This crate includes end to end, highly commented examples of how to use
various DataFusion APIs to help you get started.

## Prerequisites

Run `git submodule update --init` to init test files.

## Running Examples

To run an example, use the `cargo run` command, such as:

```bash
git clone https://github.com/apache/datafusion
cd datafusion
# Download test data
git submodule update --init

# Change to the examples directory
cd datafusion-examples/examples

# Run all examples in a group
cargo run --example <group> -- all

# Run a specific example within a group
cargo run --example <group> -- <subcommand>

# Run all examples in the `dataframe` group
cargo run --example dataframe -- all

# Run a single example from the `dataframe` group
# (apply the same pattern for any other group)
cargo run --example dataframe -- dataframe
```

## Example Metadata (`examples.toml`)

The list of examples and their documentation is generated automatically from
the filesystem and the `examples.toml` file located in this crate.

### What is `examples.toml`?

`examples.toml` defines **human-readable metadata** for examples, including:

- the subcommand name used with `cargo run --example <group> -- <subcommand>`
- a short description explaining what the example demonstrates
- the Rust source file that implements the example

The actual source files under `examples/<group>/*.rs` remain the source of truth
for which examples exist.

### Important notes

- the subcommand name does not have to match the filename
- subcommands are a stable CLI interface; filenames are free to change
- all files referenced in examples.toml must exist on disk (validated by CI)

### Format

Each section corresponds to an example group and maps subcommands to files:

```toml
[dataframe]
dataframe = { file = "dataframe.rs", desc = "Query DataFrames from various sources" }
cache_factory = { file = "cache_factory.rs", desc = "Custom lazy caching for DataFrames" }
```
EOF

# ------------------------------------------------------------------------------
# Auto-generated examples section
# ------------------------------------------------------------------------------

# Generates documentation for a single example group
generate_group_docs() {
  local group="$1"
  local category="$DEFAULT_CATEGORY"

  is_distributed "$group" && category="Distributed"
  check_group_consistency "$group"

  echo
  echo "## $(title_case "$group") Examples"
  echo
  echo "### Group: \`${group}\`"
  echo
  echo "#### Category: ${category}"
  echo
  echo "| Subcommand | File Path | Description |"
  echo "| --- | --- | --- |"

  # Iterate over all example files on disk (excluding main.rs)
  find "$EXAMPLES_DIR/$group" -maxdepth 1 -name "*.rs" ! -name "main.rs" \
    -exec basename {} \; | sort | while read -r file; do

    # Read metadata from TOML
    metadata="$(get_metadata_by_file "$group" "$file")"

    # Split into subcommand and description
    subcommand="${metadata%%|*}"
    desc="${metadata#*|}"

    # Default subcommand to filename if not specified
    [[ -z "$subcommand" ]] && subcommand="${file%.rs}"

    # Trim whitespace defensively
    subcommand="$(echo "$subcommand" | xargs)"
    desc="$(echo "$desc" | xargs)"

    # Print Markdown row
    echo "| ${subcommand} | [\`${group}/${file}\`](examples/${group}/${file}) | ${desc} |"
  done
}

# ------------------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------------------

# Generate docs for a single group or all groups
if [[ -n "$ONLY_GROUP" ]]; then
  skip "$ONLY_GROUP" && exit 0
  generate_group_docs "$ONLY_GROUP"
else
  for group in $(find "$EXAMPLES_DIR" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort); do
    skip "$group" && continue
    generate_group_docs "$group"
  done
fi
