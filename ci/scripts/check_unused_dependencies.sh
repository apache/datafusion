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

# Detects unused dependencies with `cargo machete`, the same way the
# "Detect Unused Dependencies" job does.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${SCRIPT_DIR}/utils/tool_versions.sh"

if ! command -v cargo-machete &> /dev/null; then
  echo "[${SCRIPT_NAME}] cargo-machete is required. Install it with: cargo install cargo-machete --locked --version ^${CARGO_MACHETE_VERSION}" >&2
  exit 1
fi

cd "${ROOT_DIR}"

echo "[${SCRIPT_NAME}] \`cargo machete --with-metadata\`"
cargo machete --with-metadata
