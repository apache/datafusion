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

# This script is meant for developers of DataFusion -- it is runnable
# from the standard DataFusion development environment and uses cargo,
# etc and orchestrates gathering data and run the benchmark binary in
# different configurations.

# Script to download ClickBench queries and split them into individual files

set -e  # Exit on any error

# URL for the raw file (not the GitHub page)
URL="https://raw.githubusercontent.com/ClickHouse/ClickBench/main/datafusion/queries.sql"

# Temporary file to store downloaded content
TEMP_FILE="queries.sql"

TARGET_DIR="queries"

# Download the file
echo "Downloading queries from $URL..."
if command -v curl &> /dev/null; then
    curl -s -o "$TEMP_FILE" "$URL"
elif command -v wget &> /dev/null; then
    wget -q -O "$TEMP_FILE" "$URL"
else
    echo "Error: Neither curl nor wget is available. Please install one of them."
    exit 1
fi

# Check if download was successful
if [ ! -f "$TEMP_FILE" ] || [ ! -s "$TEMP_FILE" ]; then
    echo "Error: Failed to download or file is empty"
    exit 1
fi

# Initialize counter
counter=0

# Ensure the target directory exists
if [ ! -d ${TARGET_DIR} ]; then
  mkdir -p ${TARGET_DIR}
fi

# Read the file line by line and create individual query files
mapfile -t lines < $TEMP_FILE
for line in "${lines[@]}"; do
    # Skip empty lines
    if [ -n "$line" ]; then
        # Create filename with zero-padded counter
        filename="q${counter}.sql"

        # Write the line to the individual file
        echo "$line" > "${TARGET_DIR}/$filename"

        echo "Created ${TARGET_DIR}/$filename"

        # Increment counter
        (( counter += 1 ))
    fi
done

# Clean up temporary file
rm "$TEMP_FILE"