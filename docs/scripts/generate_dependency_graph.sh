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
SVG_OUTPUT="${OUTPUT_DIR}/deps.svg"

usage() {
  cat <<EOF
Generate the workspace dependency graph SVG for the docs.

'deps.svg' is embedded in the DataFusion docs (Contributor Guide → Architecture → Workspace Dependency Graph).

Output:
  SVG: ${SVG_OUTPUT}

Usage: $(basename "$0")

Options:
  -h, --help  Show this help message.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

if ! command -v cargo-depgraph > /dev/null 2>&1; then
  echo "cargo-depgraph is required (install with: cargo install cargo-depgraph)." >&2
  exit 1
fi

if ! command -v dot >/dev/null 2>&1; then
  echo "Graphviz 'dot' is required to render the SVG." >&2
  exit 1
fi

mkdir -p "${OUTPUT_DIR}"

(
  cd "${REPO_DIR}"
  # Ignore utility crates only used by internal scripts
  cargo depgraph \
    --workspace-only \
    --all-deps \
    --dedup-transitive-deps \
    --exclude gen,gen-common \
    | dot \
      -Grankdir=TB \
      -Gconcentrate=true \
      -Goverlap=false \
      -Tsvg \
      > "${SVG_OUTPUT}"
)

echo "Wrote dependency graph SVG to ${SVG_OUTPUT}"
