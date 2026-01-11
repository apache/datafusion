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

ROOT_DIR="$(git rev-parse --show-toplevel)"
EXAMPLES_DIR="$ROOT_DIR/datafusion-examples"
README="$EXAMPLES_DIR/README.md"
README_NEW="$EXAMPLES_DIR/README-NEW.md"

echo "▶ Generating examples README…"
bash "$ROOT_DIR/ci/scripts/generate_examples_docs.sh" > "$README_NEW"

echo "▶ Formatting generated README with DataFusion's Prettier…"
bash "$ROOT_DIR/ci/scripts/doc_prettier_check.sh" --write "$README_NEW"

echo "▶ Comparing generated README with committed version…"

if ! diff -u "$README" "$README_NEW" > /tmp/examples-readme.diff; then
  echo ""
  echo "❌ Examples README is out of date."
  echo ""
  echo "The documentation for examples is generated automatically from:"
  echo "  - examples/<group>/*.rs"
  echo "  - datafusion-examples/examples.toml"
  echo ""
  echo "💡 Note: If the README is out of date, please make sure examples.toml is up-to-date first, then regenerate the README."
  echo ""
  echo "To update locally (after fixing examples.toml):"
  echo ""
  echo "  bash ci/scripts/generate_examples_docs.sh \\"
  echo "    | bash ci/scripts/doc_prettier_check.sh --write \\"
  echo "    > datafusion-examples/README.md"
  echo ""
  echo "Diff:"
  echo "------------------------------------------------------------"
  cat /tmp/examples-readme.diff
  echo "------------------------------------------------------------"
  exit 1
fi

echo "✅ Examples README is up-to-date."
