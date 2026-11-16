#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"

cd "${ROOT_DIR}"

MARKDOWN_FILES=()
while IFS= read -r file; do
  MARKDOWN_FILES+=("${file}")
done < <(
  git -C "${ROOT_DIR}" ls-files 'README.md' 'CONTRIBUTING.md' 'docs/**/*.md' 'datafusion-cli/README.md' 'datafusion-examples/README.md' 'dev/**/*.md'
)

lychee --no-progress --config "${ROOT_DIR}/lychee.toml" "${MARKDOWN_FILES[@]}"
