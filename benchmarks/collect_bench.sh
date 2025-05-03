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

# Exit on error
set -e


# Collect benchmark results for different datafusion releases.
#
# Usage: collect_bench.sh <bench_name>
#
# `<bench_name>`: an argument to bench.sh
#
# Example:
# collect_bench.sh clickbench
#
#
# The script uses cargo to check out and run the benchmark binary for
# from current main and last 5 major releases (based on tag name)
# The results are stored in the `results` directory.
#
# By default the current datafusion checkout is used. If you want to use a different
# checkout, set the DATAFUSION_DIR environment variable:
#
# Example:
# DATAFUSION_DIR=/path/to/checkout collect_bench.sh clickbench

BENCH_NAME=$1

if [ -z "$BENCH_NAME" ] ; then
    echo "USAGE: collect_bench.sh <bench_name>"
    exit 1
fi

DATAFUSION_DIR=${DATAFUSION_DIR:-$SCRIPT_DIR/..}
echo "Running $BENCH_NAME"
echo "Using DATAFUSION_DIR: $DATAFUSION_DIR"

pushd $DATAFUSION_DIR

git checkout main

# get current major version 
output=$(cargo metadata --format-version=1 --no-deps | jq '.packages[] | select(.name == "datafusion") | .version')
major_version=$(echo "$output" | grep -oE '[0-9]+' | head -n1)

# run for current main
echo "current major version: $major_version"  
export RESULTS_DIR="results/main"
./benchmarks/bench.sh run $BENCH_NAME

# run for last 5 major releases
for i in {1..5}; do
    echo "running benchmark on  $((major_version-i)).0.0"
    git checkout $((major_version-i)).0.0
    export RESULTS_DIR="results/$((major_version-i)).0.0"
    ./benchmarks/bench.sh run $BENCH_NAME
done

# restore working directory
popd