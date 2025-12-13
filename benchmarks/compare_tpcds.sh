#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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