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

# See `usage()` for details about this script.
#
# The key commands to generate the dependency graph SVG in this script are:
#   cargo depgraph ... | dot -Tsvg > deps.svg
# See below for the exact command used.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_DIR="${REPO_DIR}/docs/source/_static/data"
DOT_OUTPUT="${OUTPUT_DIR}/deps.dot"
SVG_OUTPUT="${OUTPUT_DIR}/deps.svg"

usage() {
  cat <<EOF
Generate the workspace dependency graph SVG for the docs.

'deps.svg' is embedded in the DataFusion docs (Contributor Guide → Architecture → Workspace Dependency Graph).

'deps.dot' is the intermediate DOT used to render deps.svg; CI checks it to ensure the graph is up to date with the codebase.

Outputs:
  DOT: ${DOT_OUTPUT}
  SVG: ${SVG_OUTPUT}

Usage: $(basename "$0") [--check]

Options:
  --check     Validate the checked-in graph without modifying files.
  -h, --help  Show this help message.
EOF
}

# Default 'generate' mode creates/overrides 'deps.dot' and 'deps.svg', and the 'check'
# mode verify if the existing 'deps.dot' is up-to-date
mode="generate"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)
      mode="check"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required to build the dependency graph." >&2
  exit 1
fi

if ! cargo depgraph --help >/dev/null 2>&1; then
  echo "cargo-depgraph is required (install with: cargo install cargo-depgraph)." >&2
  exit 1
fi

if [[ "${mode}" != "check" ]] && ! command -v dot >/dev/null 2>&1; then
  echo "Graphviz 'dot' is required to render the SVG." >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

generate_dot() {
  local output_file="$1"
  (
    cd "${REPO_DIR}"
    cargo depgraph \
      --workspace-only \
      --all-deps \
      --dedup-transitive-deps \
      > "${output_file}"
  )
}

if [[ "${mode}" == "check" ]]; then
  if [[ ! -f "${DOT_OUTPUT}" ]]; then
    echo "Expected graph ${DOT_OUTPUT} is missing; regenerate it with $(basename "$0")." >&2
    exit 1
  fi

  temp_dot="$(mktemp)"
  cleanup() { rm -f "${temp_dot}"; }
  trap cleanup EXIT

  generate_dot "${temp_dot}"

  if ! cmp -s "${temp_dot}" "${DOT_OUTPUT}"; then
    echo "Dependency graph is out of date. Re-run $(basename "$0") to refresh ${DOT_OUTPUT} and ${SVG_OUTPUT}." >&2
    exit 1
  fi

  echo "Dependency graph is up to date."
  exit 0
fi

generate_dot "${DOT_OUTPUT}"

dot \
  -Grankdir=TB \
  -Gconcentrate=true \
  -Goverlap=false \
  -Tsvg "${DOT_OUTPUT}" \
  > "${SVG_OUTPUT}"

echo "Wrote dependency graph DOT to ${DOT_OUTPUT}"
echo "Wrote dependency graph SVG to ${SVG_OUTPUT}"
