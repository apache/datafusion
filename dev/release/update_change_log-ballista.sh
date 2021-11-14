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
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log-ballista.sh

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

CURRENT_VER=$(grep version "${SOURCE_TOP_DIR}/ballista/rust/client/Cargo.toml" | head -n 1 | awk '{print $3}' | tr -d '"')
${SOURCE_DIR}/update_change_log.sh \
    ballista \
    ballista-0.5.0 \
    --exclude-tags-regex "python-.+" \
    --future-release "ballista-${CURRENT_VER}"
