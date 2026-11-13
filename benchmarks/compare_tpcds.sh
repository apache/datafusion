#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

# Compare TPC-DS benchmarks between two branches

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

usage() {
    echo "Usage: $0 <branch1> <branch2>"
    echo ""
    echo "Example: $0 main dev2"
    echo ""
    echo "Note: TPC-DS benchmarks are not currently implemented in bench.sh"
    exit 1
}

BRANCH1=${1:-""}
BRANCH2=${2:-""}

if [ -z "$BRANCH1" ] || [ -z "$BRANCH2" ]; then
    usage
fi

# Store current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)

echo "Comparing TPC-DS benchmarks: ${BRANCH1} vs ${BRANCH2}"

# Run benchmark on first branch
git checkout "$BRANCH1"
./benchmarks/bench.sh run tpcds

# Run benchmark on second branch
git checkout "$BRANCH2"
./benchmarks/bench.sh run tpcds

# Compare results
./benchmarks/bench.sh compare "$BRANCH1" "$BRANCH2"

# Return to original branch
git checkout "$CURRENT_BRANCH"