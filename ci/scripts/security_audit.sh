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

# Audits the root `Cargo.lock` with `cargo audit`, the same way the
# "Security audit" GitHub workflow does.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if ! command -v cargo-audit &> /dev/null; then
  echo "[${SCRIPT_NAME}] cargo-audit is required. Install it with: cargo install cargo-audit --locked" >&2
  exit 1
fi

# RUSTSEC-2026-0194 and RUSTSEC-2026-0195 are in quick-xml, reached through
# object_store. Remove them once object_store upgrades to quick-xml >= 0.41.0.
# https://github.com/apache/datafusion/issues/23297
IGNORED_ADVISORIES=(
  RUSTSEC-2026-0194
  RUSTSEC-2026-0195
)

IGNORE_ARGS=()
for advisory in "${IGNORED_ADVISORIES[@]}"; do
  IGNORE_ARGS+=(--ignore "${advisory}")
done

cd "${ROOT_DIR}"

echo "[${SCRIPT_NAME}] \`cargo audit ${IGNORE_ARGS[*]}\`"
# The guarded expansion keeps `set -u` happy on bash 3.2 (macOS) if the list is empty.
cargo audit ${IGNORE_ARGS[@]+"${IGNORE_ARGS[@]}"}
