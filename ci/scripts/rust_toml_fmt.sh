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

# Run cargo-tomlfmt with flag `-d` in dry run to check formatting
# without overwritng the file. If any error occur, you may want to
# rerun 'cargo tomlfmt -p path/to/Cargo.toml' without '-d' to fix
# the formatting automatically.
set -ex
for toml in $(find . -mindepth 2 -name 'Cargo.toml'); do
	cargo tomlfmt -d -p $toml
done
