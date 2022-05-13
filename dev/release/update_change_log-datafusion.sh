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

# Usage:
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log-datafusion.sh master 8.0.0 7.1.0
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log-datafusion.sh maint-7.x 7.1.0 7.0.0

RELEASE_BRANCH=$1
RELEASE_TAG=$2
BASE_TAG=$3

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
${SOURCE_DIR}/update_change_log.sh \
    datafusion \
    "${BASE_TAG}" \
    --exclude-tags-regex "(python|ballista)-.+" \
    --future-release "${RELEASE_TAG}" \
    --release-branch "${RELEASE_BRANCH}"
