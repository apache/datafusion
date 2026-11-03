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

# Checks for circular dependencies between DataFusion crates, the same way the
# "Circular Dependency Check" job does.
#
# A cycle prevents publishing the crates to crates.io. A crate can be
# published only after every DataFusion crate it depends on, including through
# dev-dependencies, optional dependencies, and dependencies for other targets,
# so no crate in a cycle can be published first. Cargo refuses to build a
# cycle of normal dependencies, but it allows one that passes through a
# dev-dependency, because a dev-dependency is built only for the crate's own
# tests, examples, and benchmarks. Such a cycle builds and passes tests, and
# fails only at release time.
#
# The check is a publishing dry run. `cargo publish --workspace` packages every
# crate that does not set `publish = false`, in an order where each crate comes
# after the DataFusion crates it depends on, and fails if there is no such
# order. `--dry-run` stops before uploading, and `--no-verify` skips building
# the packages. The dry run also fails on any other problem that would stop the
# crates from being published, such as a dependency without a version.
# `--allow-dirty` lets the check run on a working tree with uncommitted
# changes.

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${ROOT_DIR}"

echo "[${SCRIPT_NAME}] \`cargo publish --workspace --dry-run --no-verify --allow-dirty\`"
cargo publish --workspace --dry-run --no-verify --allow-dirty
