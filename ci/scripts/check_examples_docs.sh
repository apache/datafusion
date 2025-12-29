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

set -euo pipefail

EXAMPLES_DIR="datafusion-examples/examples"
README="datafusion-examples/README.md"

# ffi examples are skipped because they were not part of the recent example
# consolidation work and do not follow the new grouping and execution pattern.
# They are not documented in the README using the new structure, so including
# them here would cause false CI failures.
SKIP_LIST=("ffi")

missing=0

skip() {
    local value="$1"
    for item in "${SKIP_LIST[@]}"; do
        if [[ "$item" == "$value" ]]; then
            return 0
        fi
    done
    return 1
}

# collect folder names
folders=$(find "$EXAMPLES_DIR" -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)

# collect group names from README headers
groups=$(grep "^### Group:" "$README" | sed -E 's/^### Group: `([^`]+)`.*/\1/')

for folder in $folders; do
    if skip "$folder"; then
        echo "Skipped group: $folder"
        continue
    fi

    if ! echo "$groups" | grep -qx "$folder"; then
        echo "Missing README entry for example group: $folder"
        missing=1
    fi
done

if [[ $missing -eq 1 ]]; then
    echo "README is out of sync with examples"
    exit 1
fi
