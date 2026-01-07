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

set -ex

# Install nightly toolchain (skips if already installed)
rustup toolchain install nightly --component rustfmt

# Use nightly rustfmt to check formatting including doc comments
# This requires nightly because format_code_in_doc_comments is an unstable feature
if ! cargo +nightly fmt --all -- --check --config format_code_in_doc_comments=true; then
    echo "To fix, run: cargo +nightly fmt --all -- --config format_code_in_doc_comments=true"
    exit 1
fi
