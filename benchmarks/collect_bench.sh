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

# Collect benchmark results for different datafusion releases.
#
# Usage: collect_bench.sh <bench_name>
#
# `<bench_name>`: an argument to bench.sh
#
# Example:
# collect_bench.sh clickbench
#
# This script is designed for developers of DataFusion to track the performance
# of DataFusion over time.
#
# Run this from the standard DataFusion development environment.
#
# The script uses cargo to check out and run run the benchmark binary to
# collect benchmarks from current main and last 5 major releases (checks out tags)

BENCH_NAME=$1

if [ -z "$BENCH_NAME" ] ; then
    echo "USAGE: collect_bench.sh <bench_name>"
    exit 1
fi

main(){

git fetch upstream main
git checkout main

# get current major version 
output=$(cargo metadata --format-version=1 --no-deps | jq '.packages[] | select(.name == "datafusion") | .version')
major_version=$(echo "$output" | grep -oE '[0-9]+' | head -n1)

# run for current main
echo "current major version: $major_version"  
export RESULTS_DIR="results/main"
./bench.sh run $BENCH_NAME

# run for last 5 major releases
for i in {1..5}; do
    echo "running benchmark on  $((major_version-i)).0.0"
    git fetch upstream $((major_version-i)).0.0
    git checkout $((major_version-i)).0.0
    export RESULTS_DIR="results/$((major_version-i)).0.0"
    ./bench.sh run $BENCH_NAME
done
}

main