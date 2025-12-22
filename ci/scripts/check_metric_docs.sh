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
# software distributed under this License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

echo "$SCRIPT_PATH: Checking if metrics.md is up-to-date"

# Ensure we're in the project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

METRICS_FILE="docs/source/user-guide/metrics.md"
METRICS_BACKUP=$(mktemp)

if [ ! -f "$METRICS_FILE" ]; then
    echo "Warning: $METRICS_FILE not found, skipping check." >&2
    rm -f "$METRICS_BACKUP"
    exit 0
fi

# Backup the current file
cp "$METRICS_FILE" "$METRICS_BACKUP"

# Run the update script (this will modify the file)
# Suppress output but capture exit code
if ! ./dev/update_metric_docs.sh > /dev/null 2>&1; then
    echo "Error: Failed to run update_metric_docs.sh. Check permissions and try again." >&2
    # Restore the original file
    mv "$METRICS_BACKUP" "$METRICS_FILE"
    exit 1
fi

# Compare the updated file with the backup
if ! diff -q "$METRICS_BACKUP" "$METRICS_FILE" > /dev/null; then
    echo "Error: metrics.md is not up-to-date. Run './dev/update_metric_docs.sh' and commit the changes." >&2
    # Restore the original file
    mv "$METRICS_BACKUP" "$METRICS_FILE"
    exit 1
fi

# Clean up the backup file
rm -f "$METRICS_BACKUP"
exit 0

