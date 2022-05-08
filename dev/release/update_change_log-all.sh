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

set -e

# Usage:
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log-all.sh <branch> <release_tag> <base_tag>
# Example:
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log-all.sh master 8.0.0 7.1.0
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log-all.sh maint-7.x 7.1.0 7.0.0

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

${SOURCE_DIR}/update_change_log-datafusion.sh $1 $2 $3
${SOURCE_DIR}/update_change_log-ballista.sh $1 $2 $3
