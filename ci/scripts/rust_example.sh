#!/usr/bin/env bash
#
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

set -e

export CARGO_PROFILE_CI_OPT_LEVEL="s"
export CARGO_PROFILE_CI_STRIP=true

# Remove leftover files inside test_* directories inside datafusion-examples/
EXAMPLES_ROOT="datafusion-examples"

find "${EXAMPLES_ROOT}" -maxdepth 1 -type d -name "test_*" | while read -r test_dir; do
    echo "Cleaning directory: $test_dir"
    find "$test_dir" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
done

# Run the examples
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
