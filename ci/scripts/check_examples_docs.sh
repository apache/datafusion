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

# Generates documentation for DataFusion examples using the Rust-based
# documentation generator and verifies that the committed README.md
# is up to date.
#
# The README is generated from documentation comments in:
#   datafusion-examples/examples/<group>/main.rs
#
# This script is intended to be run in CI to ensure that example
# documentation stays in sync with the code.
#
# To update the README locally, run this script and replace README.md
# with the generated output.

set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"

# Load centralized tool versions
source "${ROOT_DIR}/ci/scripts/utils/tool_versions.sh"

EXAMPLES_DIR="$ROOT_DIR/datafusion-examples"
README="$EXAMPLES_DIR/README.md"
README_NEW="$EXAMPLES_DIR/README-NEW.md"

echo "▶ Generating examples README (Rust generator)…"
cargo run --quiet \
  --manifest-path "$EXAMPLES_DIR/Cargo.toml" \
  --bin examples-docs \
  > "$README_NEW"

echo "▶ Formatting generated README with prettier ${PRETTIER_VERSION}…"
npx "prettier@${PRETTIER_VERSION}" \
  --parser markdown \
  --write "$README_NEW"

echo "▶ Comparing generated README with committed version…"

if ! diff -u "$README" "$README_NEW" > /tmp/examples-readme.diff; then
  echo ""
  echo "❌ Examples README is out of date."
  echo ""
  echo "The examples documentation is generated automatically from:"
  echo "  - datafusion-examples/examples/<group>/main.rs"
  echo ""
  echo "To update the README locally, run:"
  echo ""
  echo "  cargo run --bin examples-docs \\"
  echo "    | npx prettier@${PRETTIER_VERSION} --parser markdown --write \\"
  echo "    > datafusion-examples/README.md"
  echo ""
  echo "Diff:"
  echo "------------------------------------------------------------"
  cat /tmp/examples-readme.diff
  echo "------------------------------------------------------------"
  exit 1
fi

echo "✅ Examples README is up-to-date."
