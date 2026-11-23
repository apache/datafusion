#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# Prints the publishable workspace crates that are not on crates.io yet.
# Run it before `cargo publish`. See release/README.md.

set -euo pipefail
cd "$(dirname "$0")/../.."

for crate in $(cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.publish != []) | .name'); do
  # Sparse index path for crate names of 4 or more characters
  status=$(curl -s -o /dev/null -w '%{http_code}' "https://index.crates.io/${crate:0:2}/${crate:2:2}/$crate")
  case $status in
    200) ;;
    404) echo "$crate" ;;
    *) echo "error: HTTP $status for $crate" >&2; exit 1 ;;
  esac
done
