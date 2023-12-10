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

# This script publishes datafusion crates to crates.io.
#
# This script should only be run after the release has been approved
# by the arrow PMC committee.
#
# See release/README.md for full release instructions

set -eu

# Do not run inside a git repo
if ! [ git rev-parse --is-inside-work-tree ]; then
  cd datafusion/common && cargo publish
  cd datafusion/expr && cargo publish
  cd datafusion/sql && cargo publish
  cd datafusion/physical-expr && cargo publish
  cd datafusion/optimizer && cargo publish
  cd datafusion/core && cargo publish
  cd datafusion/proto && cargo publish
  cd datafusion/execution && cargo publish
  cd datafusion/substrait && cargo publish
  cd datafusion-cli && cargo publish --no-verify
else
    echo "Crates must be released from the source tarball that was voted on, not from the repo"
    exit 1
fi
