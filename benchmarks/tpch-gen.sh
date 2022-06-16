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

#set -e

pushd ..
. ./dev/build-set-env.sh
popd

docker build  -f tpchgen.dockerfile -t datafusion-tpchgen:$DATAFUSION_VERSION .

# Generate data into the ./data directory if it does not already exist
FILE=./data/supplier.tbl
if test -f "$FILE"; then
    echo "$FILE exists."
else
  mkdir data 2>/dev/null
  docker run datafusion-tpchgen:$DATAFUSION_VERSION $1 -v `pwd`/data:/data -it --rm datafusion-tpchgen:$DATAFUSION_VERSION
  ls -l data
fi