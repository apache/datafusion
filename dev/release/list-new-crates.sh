#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# Prints the publishable workspace crates that are not yet on crates.io,
# one per line. Run it before `cargo publish` so the release manager knows
# which crates need owners added after publishing.
#
# See release/README.md for full release instructions

set -euo pipefail

cd "$(dirname "$0")/../.."

for crate in $(cargo metadata --no-deps --format-version 1 |
  jq -r '.packages[] | select(.publish != []) | .name'); do
  # Path layout of the sparse index: https://doc.rust-lang.org/cargo/reference/registry-index.html#index-files
  name=$(echo "$crate" | tr '[:upper:]' '[:lower:]')
  case ${#name} in
    1) path="1/$name" ;;
    2) path="2/$name" ;;
    3) path="3/${name:0:1}/$name" ;;
    *) path="${name:0:2}/${name:2:2}/$name" ;;
  esac
  status=$(curl -s -o /dev/null -w '%{http_code}' "https://index.crates.io/$path")
  case $status in
    200) ;;
    404) echo "$crate" ;;
    *) echo "error: crates.io index returned HTTP $status for $crate" >&2; exit 1 ;;
  esac
done
