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

# Fix line endings when running (for volume-mounted files from Windows) only if needed
if find /work -name '*.sh' -type f -print0 | xargs -0 grep -Il $'\r' >/dev/null 2>&1; then
    echo "Converting CRLF to LF in shell scripts..."
    find /work -name '*.sh' -type f -exec sh -c 'tr -d "\r" < "$1" > "$1.tmp" && mv "$1.tmp" "$1"' _ {} \;
fi

# Build documentation
cd /work/docs
bash build.sh
