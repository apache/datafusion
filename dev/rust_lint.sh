#!/bin/bash

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

# This script runs all the Rust lints locally the same way the
# DataFusion CI does
#
# Note: The installed checking tools (e.g., taplo) are not guaranteed to match
# the CI versions for simplicity, there might be some minor differences. Check
# `.github/workflows` for the CI versions.

# For `.toml` format checking
set -e
if ! command -v taplo &> /dev/null; then
    echo "Installing taplo using cargo"
    cargo install taplo-cli
fi

# For Apache licence header checking
if ! command -v hawkeye &> /dev/null; then
    echo "Installing hawkeye using cargo"
    cargo install hawkeye --locked
fi

# For spelling checks
if ! command -v typos &> /dev/null; then
    echo "Installing typos using cargo"
    cargo install typos-cli --locked
fi

ci/scripts/rust_fmt.sh
ci/scripts/rust_clippy.sh
ci/scripts/rust_toml_fmt.sh
ci/scripts/rust_docs.sh
ci/scripts/license_header.sh
ci/scripts/typos_check.sh
