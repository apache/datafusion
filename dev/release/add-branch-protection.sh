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

set -e

# Script to add branch protection for a new release branch in .asf.yaml
#
# This script automates the process of adding branch protection rules to .asf.yaml
# for new release branches. It ensures the branch protection block doesn't already
# exist before adding it.
#
# Usage:
#   ./dev/release/add-branch-protection.sh <release_number>
#
# Examples:
#   ./dev/release/add-branch-protection.sh 52
#   ./dev/release/add-branch-protection.sh 53
#
# The script will:
#   1. Validate the release number is a positive integer
#   2. Check if branch protection already exists for branch-<release_number>
#   3. Add the branch protection block to .asf.yaml if it doesn't exist
#   4. Error out if the block already exists

# Check if release number is provided
if [ $# -eq 0 ]; then
    echo "Error: Release number is required"
    echo "Usage: $0 <release_number>"
    echo "Example: $0 52"
    exit 1
fi

RELEASE_NUM=$1
BRANCH_NAME="branch-${RELEASE_NUM}"
ASF_YAML_FILE=".asf.yaml"

# Validate release number is a positive integer
if ! [[ "$RELEASE_NUM" =~ ^[0-9]+$ ]]; then
    echo "Error: Release number must be a positive integer"
    echo "Provided: $RELEASE_NUM"
    echo "Example: ./dev/release/add-branch-protection.sh 52"
    exit 1
fi

# Check if .asf.yaml exists
if [ ! -f "$ASF_YAML_FILE" ]; then
    echo "Error: $ASF_YAML_FILE not found in current directory"
    echo "Please run this script from the repository root"
    exit 1
fi

# Check if the branch exists in the official Apache DataFusion repository
GITHUB_API_URL="https://api.github.com/repos/apache/datafusion/branches/${BRANCH_NAME}"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$GITHUB_API_URL")

if [ "$HTTP_STATUS" != "200" ]; then
    echo "Error: Branch ${BRANCH_NAME} does not exist in the official Apache DataFusion repository"
    echo "Please create the branch '${BRANCH_NAME}' first before adding branch protection"
    echo ""
    echo "To check existing branches, visit:"
    echo "  https://github.com/apache/datafusion/branches"
    exit 1
fi

# Check if branch protection already exists for this release
if grep -q "^[[:space:]]*${BRANCH_NAME}:" "$ASF_YAML_FILE"; then
    echo "Error: Branch protection for ${BRANCH_NAME} already exists in $ASF_YAML_FILE"
    exit 1
fi

# Create a temporary file
TEMP_FILE=$(mktemp)

# Read the file and insert the new branch protection block
# We'll insert it after the last branch-XX block
awk -v branch="$BRANCH_NAME" '
/^[[:space:]]*branch-[0-9]+:/ {
    last_branch_line = NR
    last_branch_content = $0
}
{
    lines[NR] = $0
}
END {
    if (last_branch_line == 0) {
        print "Error: No existing branch protection blocks found" > "/dev/stderr"
        exit 1
    }
    
    # Print all lines up to and including the last branch block
    for (i = 1; i <= last_branch_line; i++) {
        print lines[i]
    }
    
    # Print the required_pull_request_reviews lines after the last branch
    for (i = last_branch_line + 1; i <= NR; i++) {
        print lines[i]
        # After printing the required_approving_review_count line, insert new branch
        if (lines[i] ~ /required_approving_review_count:/) {
            # Check if this belongs to the last branch block by looking ahead
            next_non_empty = i + 1
            while (next_non_empty <= NR && lines[next_non_empty] ~ /^[[:space:]]*$/) {
                next_non_empty++
            }
            # If next non-empty line is not indented more than branch level, we found the end
            if (next_non_empty > NR || lines[next_non_empty] !~ /^[[:space:]]{6,}/) {
                print "    " branch ":"
                print "      required_pull_request_reviews:"
                print "        required_approving_review_count: 1"
                # Skip to next iteration to avoid double printing
                for (j = i + 1; j <= NR; j++) {
                    i = j
                    if (j <= NR) print lines[j]
                }
                break
            }
        }
    }
}
' "$ASF_YAML_FILE" > "$TEMP_FILE"

# Check if awk succeeded
if [ $? -ne 0 ]; then
    rm -f "$TEMP_FILE"
    exit 1
fi

# Verify the new content was added
if ! grep -q "^[[:space:]]*${BRANCH_NAME}:" "$TEMP_FILE"; then
    echo "Error: Failed to add branch protection block"
    rm -f "$TEMP_FILE"
    exit 1
fi

# Replace the original file with the modified version
mv "$TEMP_FILE" "$ASF_YAML_FILE"

echo "Successfully added branch protection for ${BRANCH_NAME} to $ASF_YAML_FILE"
echo ""
echo "Added block:"
echo "    ${BRANCH_NAME}:"
echo "      required_pull_request_reviews:"
echo "        required_approving_review_count: 1"
echo ""
echo "Please review the changes and commit them."