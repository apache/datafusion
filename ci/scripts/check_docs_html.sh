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

# Builds the documentation website with `docs/build.sh`, the same way the
# "Test doc build" job does. Sphinx runs with `-W`, so a warning fails the
# build.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  cat >&2 <<USAGE
Usage: $0

Builds the HTML documentation with docs/build.sh and fails on any Sphinx
warning. There is no write mode.

Needs uv, cargo, cargo-depgraph, Graphviz dot, and make. See docs/README.md
for the Python and Graphviz setup.

Each build rewrites docs/build and docs/source/_static/data/deps.svg, which
Git ignores. The HTML entry point is docs/build/html/index.html.
USAGE
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
  shift
done

# Report every missing prerequisite in one run. This check installs nothing.
missing=0
require_tool() {
  local cmd="$1"
  local hint="$2"
  if ! command -v "$cmd" > /dev/null 2>&1; then
    echo "[${SCRIPT_NAME}] ${cmd} was not found on PATH. ${hint}" >&2
    missing=1
  fi
}

require_tool "uv" "Install uv to get Python and the documentation dependencies: https://docs.astral.sh/uv/getting-started/installation/"
require_tool "cargo" "Install the Rust toolchain with rustup: https://rustup.rs/"
require_tool "cargo-depgraph" "Install it with: cargo install cargo-depgraph --version '^1.6' --locked"
require_tool "dot" "Install Graphviz (e.g., brew install graphviz, or apt-get install graphviz)."
require_tool "make" "Install make (e.g., xcode-select --install, or apt-get install make)."

if [[ "${missing}" -ne 0 ]]; then
  exit 1
fi

cd "${ROOT_DIR}"

echo "[${SCRIPT_NAME}] Building the documentation website with docs/build.sh"

# `docs/build.sh` owns the dependency graph and Sphinx commands and moves to
# the docs directory itself.
uv run --package datafusion-docs ./docs/build.sh

echo "[${SCRIPT_NAME}] Wrote the HTML documentation to docs/build/html/index.html"
