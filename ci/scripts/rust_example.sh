#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

set -e

export CARGO_PROFILE_CI_OPT_LEVEL="s"
export CARGO_PROFILE_CI_STRIP=true

cd datafusion-examples/examples/
cargo build --profile ci --examples

SKIP_LIST=("external_dependency" "flight" "ffi")

skip_example() {
    local name="$1"
    for skip in "${SKIP_LIST[@]}"; do
        if [ "$name" = "$skip" ]; then
            return 0
        fi
    done
    return 1
}

for dir in */; do
    example_name=$(basename "$dir")

    if skip_example "$example_name"; then
        echo "Skipping $example_name"
        continue
    fi

    echo "Running example group: $example_name"
    cargo run --profile ci --example "$example_name" -- all
done
