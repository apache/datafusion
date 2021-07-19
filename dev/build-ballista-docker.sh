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

set -e

. ./dev/build-set-env.sh

# If running within CI, use docker-container driver to cache build cache into
# local folders so we can share them between runs.
if [[ "${CI}" = "true" ]] && docker buildx &>/dev/null; then
    echo "building docker image in CI, saving build cache to local folder ${BUILDX_CACHE_DIR}."
    BUILDER=ballista-docker-builder
    docker buildx inspect "${BUILDER}" &>/dev/null || \
        docker buildx create --driver docker-container --name "${BUILDER}" --use
    BUILD_ARGS=(buildx build \
        --builder ${BUILDER} \
        --cache-from="type=local,src=${BUILDX_CACHE_DIR}" \
        --cache-to="type=local,mode=max,dest=${BUILDX_CACHE_DIR}" \
        --load)
else
    echo "No docker buildx plugin found, fallback to default docker build command with DOCKER_BUILDKIT=1"
    echo "To install docker buildx, follow https://github.com/docker/buildx#installing"
    export DOCKER_BUILDKIT=1
    BUILD_ARGS=(build)
fi

docker ${BUILD_ARGS[@]} -t ballista-base:$BALLISTA_VERSION -f dev/docker/ballista-base.dockerfile .
# couldn't use buildx for the ballista app image build due to restrictions of
# the docker-container driver. see https://github.com/docker/buildx/issues/301
docker build -t ballista:$BALLISTA_VERSION -f dev/docker/ballista.dockerfile .
