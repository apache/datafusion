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

repo_dir=$PWD

cd datafusion-examples/examples/
cargo fmt --all -- --check
cargo check --examples

size_threshold=$((10 * 1024 * 1024 * 1024))  # 10GB

files=$(ls .)
for filename in $files
do
  example_name=`basename $filename ".rs"`
  # Skip tests that rely on external storage and flight
  if [ ! -d $filename ]; then
     cargo run --example $example_name

     # If the examples are getting to big, run cargo clean
     current_size=$(du -s $repo_dir/target/debug | awk '{print $1}')

    if [ $current_size -gt $size_threshold ]; then
        echo "Cleaning cargo due to directory size exceeding 10 GB..."
        cargo clean
    fi

  fi
done
