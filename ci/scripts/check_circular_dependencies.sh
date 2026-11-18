#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
