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

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
echo "$SCRIPT_PATH: Checking document format with prettier"

# Ensure `npx` is available
if ! command -v npx >/dev/null 2>&1; then
  echo "npx is required to run the prettier check. Install Node.js (e.g., brew install node) and re-run." >&2
  exit 1
fi
 
# if you encounter error, change '--check' to '--write' in the below command, and
# commit the change.
#
# Ignore subproject CHANGELOG.md because it is machine generated
npx prettier@2.7.1 --check \
  '{datafusion,datafusion-cli,datafusion-examples,dev,docs}/**/*.md' \
  '!datafusion/CHANGELOG.md' \
  README.md \
  CONTRIBUTING.md
status=$?

if [ $status -ne 0 ]; then
  echo "Prettier check failed. To fix, rerun `prettier` with --write (change --check to --write in the script), commit the formatted files, and re-run the check." >&2
  exit $status
fi
