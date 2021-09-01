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
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log.sh <PROJECT> <FROM_VER> <TO_VER>

set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [[ "$#" -ne 3 ]]; then
    echo "USAGE: $0 PROJECT FROM_VER TO_VER"
    exit 1
fi

PROJECT=$1
FROM_VER=$2
TO_VER=$3
OUTPUT_PATH="${PROJECT}/CHANGELOG.md"

pushd ${SOURCE_TOP_DIR}
docker run -it --rm \
    -e CHANGELOG_GITHUB_TOKEN=$CHANGELOG_GITHUB_TOKEN \
    -v "$(pwd)":/usr/local/src/your-app \
    githubchangeloggenerator/github-changelog-generator \
    --user apache \
    --project arrow-datafusion \
    --since-tag "${FROM_VER}" \
    --include-labels "${PROJECT}" \
    --output "${OUTPUT_PATH}" \
    --future-release "${TO_VER}"

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
