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

if [ -z "${DOCKER_REPO}" ]; then
  echo "DOCKER_REPO env var must be set"
  exit -1
fi
cargo install cross
cross build --release --target aarch64-unknown-linux-gnu
rm -rf temp-ballista-docker
mkdir temp-ballista-docker
cp target/aarch64-unknown-linux-gnu/release/ballista-executor temp-ballista-docker
cp target/aarch64-unknown-linux-gnu/release/ballista-scheduler temp-ballista-docker
cp target/aarch64-unknown-linux-gnu/release/tpch temp-ballista-docker
mkdir temp-ballista-docker/queries/
cp benchmarks/queries/*.sql temp-ballista-docker/queries/
docker buildx build --push -t $DOCKER_REPO/ballista-arm64 --platform=linux/arm64 -f dev/docker/ballista-arm64.Dockerfile temp-ballista-docker
rm -rf temp-ballista-docker