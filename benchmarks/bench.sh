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

# This script is meant for developers of DataFusion -- it is runnable
# from the standard DataFusion development environment and uses cargo,
# etc.

# Exit on error
set -e

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )


# Set Defaults
COMMAND=
BENCHMARK=all
DATAFUSION_DIR=${DATAFUSION_DIR:-$SCRIPT_DIR/..}
DATA_DIR=${DATA_DIR:-$SCRIPT_DIR/data}
#CARGO_COMMAND=${CARGO_COMMAND:-"cargo run --release"}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run --profile release-nonlto"}  # TEMP: for faster iterations

usage() {
    echo "
Orchestrates running benchmarks against DataFusion checkouts

Usage:
$0 data [benchmark]
$0 run [benchmark]
$0 compare <branch1> <branch2>

**********
Examples:
**********
# Create the datasets for all benchmarks in $DATA_DIR
./bench.sh data

# Run the 'tpch' benchmark on the datafusion checkout in /source/arrow-datafusion
DATAFASION_DIR=/source/arrow-datafusion ./bench.sh run tpch

**********
* Commands
**********
data:         Generates data needed for benchmarking
run:          Runs the named benchmark
compare:      Comares results from benchmark runs

**********
* Benchmarks
**********
all(default): Data/Run/Compare for all benchmarks
tpch:         TPCH inspired benchmark on Scale Factor (SF) 1 (~1GB), single parquet file per table
tpch_mem:     TPCH inspired benchmark on Scale Factor (SF) 1 (~1GB), query from memory
tpch10:       TPCH inspired benchmark on Scale Factor (SF) 10 (~10GB), single parquet file per table
tpch10_mem:   TPCH inspired benchmark on Scale Factor (SF) 10 (~10GB), query from memory
parquet:      Benchmark of parquet reader's filtering speed
sort:         Benchmark of sorting speed

**********
* Supported Configuration (Environment Variables)
**********
DATA_DIR        directory to store datasets
CARGO_COMMAND   command that runs the benchmark binary
DATAFASION_DIR  directory to use (default $DATAFUSION_DIR)
"
    exit 1
}

# https://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash
POSITIONAL_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        # -e|--extension)
        #   EXTENSION="$2"
        #   shift # past argument
        #   shift # past value
        #   ;;
        -h|--help)
            shift # past argument
            usage
            ;;
        -*|--*)
            echo "Unknown option $1"
            exit 1
            ;;
        *)
            POSITIONAL_ARGS+=("$1") # save positional arg
            shift # past argument
            ;;
    esac
done

set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters
COMMAND=${1:-"${COMMAND}"}
ARG2=$2
ARG3=$3

# Do what is requested
main() {
    # Command Dispatch
    case "$COMMAND" in
        data)
            BENCHMARK=${ARG2:-"${BENCHMARK}"}
            echo "***************************"
            echo "DataFusion Benchmark Data Generation"
            echo "COMMAND: ${COMMAND}"
            echo "BENCHMARK: ${BENCHMARK}"
            echo "DATA_DIR: ${DATA_DIR}"
            echo "CARGO_COMMAND: ${CARGO_COMMAND}"
            echo "***************************"
            case "$BENCHMARK" in
                all)
                    data_tpch "1"
                    data_tpch "10"
                    ;;
                tpch)
                    data_tpch "1"
                    ;;
                tpch_mem)
                    # same data as for tpch
                    data_tpch "1"
                    ;;
                tpch10)
                    data_tpch "10"
                    ;;
                tpch_mem10)
                    # same data as for tpch10
                    data_tpch "10"
                    ;;
                *)
                    echo "Error: unknown benchmark '$BENCHMARK' for data generation"
                    usage
                    ;;
            esac
            ;;
        run)
            # Parse positional paraleters
            BENCHMARK=${ARG2:-"${BENCHMARK}"}
            BRANCH_NAME=$(cd ${DATAFUSION_DIR} && git rev-parse --abbrev-ref HEAD)
            BRANCH_NAME=${BRANCH_NAME//\//_} # mind blowing syntax to replace / with _
            RESULTS_DIR=${RESULTS_DIR:-"$SCRIPT_DIR/results/$BRANCH_NAME"}

            echo "***************************"
            echo "DataFusion Benchmark Script"
            echo "COMMAND: ${COMMAND}"
            echo "BENCHMARK: ${BENCHMARK}"
            echo "DATAFUSION_DIR: ${DATAFUSION_DIR}"
            echo "BRACH_NAME: ${BRANCH_NAME}"
            echo "DATA_DIR: ${DATA_DIR}"
            echo "RESULTS_DIR: ${RESULTS_DIR}"
            echo "CARGO_COMMAND: ${CARGO_COMMAND}"
            echo "***************************"

            # navigate to the appropriate directory
            pushd "${DATAFUSION_DIR}/benchmarks" > /dev/null
            mkdir -p "${RESULTS_DIR}"
            case "$BENCHMARK" in
                all)
                    run_tpch "1"
                    run_tpch_mem "1"
                    run_tpch "10"
                    run_tpch_mem "10"
                    run_parquet
                    run_sort
                    ;;
                tpch)
                    run_tpch "1"
                    ;;
                tpch_mem)
                    run_tpch_mem "1"
                    ;;
                tpch10)
                    run_tpch "10"
                    ;;
                tpch_mem10)
                    run_tpch_mem "10"
                    ;;
                parquet)
                    run_parquet
                    ;;
                sort)
                    run_sort
                    ;;
                *)
                    echo "Error: unknown benchmark '$BENCHMARK' for run"
                    usage
                    ;;
            esac
            popd > /dev/null
            echo "Done"
            ;;
        compare)
            BRANCH1=$1
            BRANCH2=$2
            compare_benchmarks
            ;;
        *)
            echo "Error: unknown command: $COMMAND"
            usage
            ;;
    esac
}



# Creates TPCH data at a certain scale factor, if it doesn't already
# exist
#
# call like: data_tpch($scale_factor)
#
# Creates data in $DATA_DIR/tpch_sf1 for scale factor 1
# Creates data in $DATA_DIR/tpch_sf10 for scale factor 10
# etc
data_tpch() {
    SCALE_FACTOR=$1
    if [ -z "$SCALE_FACTOR" ] ; then
        echo "Internal error: Scale factor not specified"
        exit 1
    fi

    TPCH_DIR="${DATA_DIR}/tpch_sf${SCALE_FACTOR}"
    echo "Creating tpch dataset at Scale Factor ${SCALE_FACTOR} in ${TPCH_DIR}..."

    # Ensure the target data directory exists
    mkdir -p "${TPCH_DIR}"

    # Create 'tbl' (CSV format) data into $DATA_DIR if it does not already exist
    FILE="${TPCH_DIR}/supplier.tbl"
    if test -f "${FILE}"; then
        echo " tbl files exist ($FILE exists)."
    else
        echo " creating tbl files with tpch_dbgen..."
        docker run -v "${TPCH_DIR}":/data -it --rm ghcr.io/databloom-ai/tpch-docker:main -vf -s ${SCALE_FACTOR}
    fi

    # Copy expected answers into the ./data/answers directory if it does not already exist
    FILE="${TPCH_DIR}/answers/q1.out"
    if test -f "${FILE}"; then
        echo " Expected answers exist (${FILE} exists)."
    else
        echo " Copying answers to ${TPCH_DIR}/answers"
        mkdir -p "${TPCH_DIR}/answers"
        docker run -v "${TPCH_DIR}":/data -it --entrypoint /bin/bash --rm ghcr.io/databloom-ai/tpch-docker:main -c "cp -f /opt/tpch/2.18.0_rc2/dbgen/answers/* /data/answers/"
    fi

    # Create 'parquet' files from tbl
    FILE="${TPCH_DIR}/supplier"
    if test -d "${FILE}"; then
        echo " parquet files exist ($FILE exists)."
    else
        echo " creating parquet files using benchmark binary ..."
        pushd "${SCRIPT_DIR}" > /dev/null
        $CARGO_COMMAND --bin tpch -- convert --input "${TPCH_DIR}" --output "${TPCH_DIR}" --format parquet
        popd > /dev/null
    fi
}

# Runs the tpch benchmark
run_tpch() {
    SCALE_FACTOR=$1
    if [ -z "$SCALE_FACTOR" ] ; then
        echo "Internal error: Scale factor not specified"
        exit 1
    fi
    TPCH_DIR="${DATA_DIR}/tpch_sf${SCALE_FACTOR}"

    RESULTS_FILE="${RESULTS_DIR}/tpch.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running tpch benchmark..."
    $CARGO_COMMAND --bin tpch -- benchmark datafusion --iterations 10 --path "${TPCH_DIR}" --format parquet -o ${RESULTS_FILE}
}

# Runs the tpch in memory
run_tpch_mem() {
    SCALE_FACTOR=$1
    if [ -z "$SCALE_FACTOR" ] ; then
        echo "Internal error: Scale factor not specified"
        exit 1
    fi
    TPCH_DIR="${DATA_DIR}/tpch_sf${SCALE_FACTOR}"

    RESULTS_FILE="${RESULTS_DIR}/tpch_mem.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running tpch_mem benchmark..."
    # -m means in memory
    $CARGO_COMMAND --bin tpch -- benchmark datafusion --iterations 10 --path "${TPCH_DIR}" -m --format parquet -o ${RESULTS_FILE}
}

# Runs the parquet filter benchmark
run_parquet() {
    RESULTS_FILE="${RESULTS_DIR}/parquet.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running parquet filter benchmark..."
    $CARGO_COMMAND --bin parquet -- filter --path "${DATA_DIR}" --scale-factor 1.0 --iterations 10 -o ${RESULTS_FILE}
}

# Runs the sort benchmark
run_sort() {
    RESULTS_FILE="${RESULTS_DIR}/sort.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running sort benchmark..."
    $CARGO_COMMAND --bin parquet -- sort --path "${DATA_DIR}" --scale-factor 1.0 --iterations 10 -o ${RESULTS_FILE}
}

compare_benchmarks() {
    BASE_RESULTS_DIR="${SCRIPT_DIR}/results"
    BRANCH1="${ARG2}"
    BRANCH2="${ARG3}"
    if [ -z "$BRANCH1" ] ; then
        echo "<branch1> not specified. Available branches:"
        ls -1 "${BASE_RESULTS_DIR}"
        exit 1
    fi

    if [ -z "$BRANCH2" ] ; then
        echo "<branch2> not specified"
        ls -1 "${BASE_RESULTS_DIR}"
        exit 1
    fi

    echo "Comparing ${BRANCH1} and ${BRANCH2}"
    for bench in `ls ${BASE_RESULTS_DIR}/${BRANCH1}` ; do
        RESULTS_FILE1="${BASE_RESULTS_DIR}/${BRANCH1}/${bench}"
        RESULTS_FILE2="${BASE_RESULTS_DIR}/${BRANCH2}/${bench}"
        if test -f "${RESULTS_FILE2}" ; then
            echo "--------------------"
            echo "Benchmark ${bench}"
            echo "--------------------"
            python3 "${SCRIPT_DIR}"/compare.py "${RESULTS_FILE1}" "${RESULTS_FILE2}"
        else
            echo "Note: Skipping ${RESULTS_FILE1} as ${RESULTS_FILE2} does not exist"
        fi
    done

}

# And start the process up
main
