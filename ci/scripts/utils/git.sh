#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# Ensure the repository is clean before auto-fixing files.
require_clean_work_tree() {
  local caller="${1:-script}"
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "[$caller] Uncommitted changes detected. Commit or stash them, or re-run with --allow-dirty." >&2
    return 1
  fi
}
