#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

set -euo pipefail

SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}")"
WORKFLOWS_DIR=".github/workflows"

if grep -R -E -w -n --include='*.yml' --include='*.yaml' -- 'cargo.*install' "${WORKFLOWS_DIR}"; then
  echo "[${SCRIPT_NAME}] Found workflow Rust tool installs that should use taiki-e/install-action instead." >&2
  exit 1
fi

echo "[${SCRIPT_NAME}] GitHub Actions workflow tool installs look good."
