#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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

cd "${ROOT_DIR}"

echo "[${SCRIPT_NAME}] \`cargo audit\`"
# The guarded expansion keeps `set -u` happy on bash 3.2 (macOS) if the list is empty.
cargo audit
