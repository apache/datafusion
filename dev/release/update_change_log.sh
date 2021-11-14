#!/bin/bash
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
#

# Adapted from https://github.com/apache/arrow-rs/tree/master/dev/release/update_change_log.sh

# invokes the changelog generator from
# https://github.com/github-changelog-generator/github-changelog-generator
#
# With the config located in
# arrow-datafusion/.github_changelog_generator
#
# Usage:
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log.sh <PROJECT> <SINCE_TAG> <EXTRA_ARGS...>

set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

echo $1

if [[ "$#" -lt 2 ]]; then
    echo "USAGE: $0 PROJECT SINCE_TAG EXTRA_ARGS..."
    exit 1
fi

PROJECT=$1
SINCE_TAG=$2
shift 2

OUTPUT_PATH="${PROJECT}/CHANGELOG.md"

pushd ${SOURCE_TOP_DIR}

# reset content in changelog
git co "${SINCE_TAG}" "${OUTPUT_PATH}"
# remove license header so github-changelog-generator has a clean base to append
sed -i '1,18d' "${OUTPUT_PATH}"

docker run -it --rm \
    -e CHANGELOG_GITHUB_TOKEN=$CHANGELOG_GITHUB_TOKEN \
    -v "$(pwd)":/usr/local/src/your-app \
    githubchangeloggenerator/github-changelog-generator:1.16.2 \
    --user apache \
    --project arrow-datafusion \
    --since-tag "${SINCE_TAG}" \
    --include-labels "${PROJECT}" \
    --base "${OUTPUT_PATH}" \
    --output "${OUTPUT_PATH}" \
    "$@"

sed -i "s/\\\n/\n\n/" "${OUTPUT_PATH}"

echo '<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
' | cat - "${OUTPUT_PATH}" > "${OUTPUT_PATH}".tmp
mv "${OUTPUT_PATH}".tmp "${OUTPUT_PATH}"
