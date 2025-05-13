#!/usr/bin/env bash
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

# This script is meant for developers of DataFusion -- it is runnable
# from the standard DataFusion development environment and uses cargo,
# etc and orchestrates gathering data and run the benchmark binary to
# collect benchmarks from the current main and last 5 major releases.

trap 'git checkout main' EXIT #checkout to main on exit
ARG1=$1

get_json_file_name() {
  if [[ "$1" == "tpch" ]]; then
    echo "tpch_sf1"
  else
    echo "$1"
  fi
}

main(){
timestamp=$(date +%s)
lp_file="results/$ARG1-$timestamp.lp"
mkdir -p results
touch $lp_file

json_file_name=$(get_json_file_name $ARG1)

cp lineprotocol.py results/

git fetch upstream main
git checkout main

# get current major version 
output=$(cargo metadata --format-version=1 --no-deps | jq '.packages[] | select(.name == "datafusion") | .version')
major_version=$(echo "$output" | grep -oE '[0-9]+' | head -n1)


# run for current main
echo "current major version: $major_version"  
export RESULTS_DIR="results/$major_version.0.0"
./bench.sh run $ARG1
python3 results/lineprotocol.py $RESULTS_DIR/$json_file_name.json >> $lp_file

# run for last 5 major releases
for i in {1..5}; do
    echo "running benchmark on $((major_version-i)).0.0"
    git fetch upstream $((major_version-i)).0.0
    git checkout $((major_version-i)).0.0
    export RESULTS_DIR="results/$((major_version-i)).0.0"
    ./bench.sh run $ARG1
    python3 results/lineprotocol.py $RESULTS_DIR/$json_file_name.json >> $lp_file
done
}

main