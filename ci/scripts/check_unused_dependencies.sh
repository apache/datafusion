#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
