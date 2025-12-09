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
# etc and orchestrates gathering data and run the benchmark binary in
# different configurations.


# Exit on error
set -e

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Execute command and also print it, for debugging purposes
debug_run() {
    set -x
    "$@"
    set +x
}

# Set Defaults
COMMAND=
BENCHMARK=all
DATAFUSION_DIR=${DATAFUSION_DIR:-$SCRIPT_DIR/..}
DATA_DIR=${DATA_DIR:-$SCRIPT_DIR/data}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run --release"}
PREFER_HASH_JOIN=${PREFER_HASH_JOIN:-true}
VIRTUAL_ENV=${VIRTUAL_ENV:-$SCRIPT_DIR/venv}

usage() {
    echo "
Orchestrates running benchmarks against DataFusion checkouts

Usage:
$0 data [benchmark]
$0 run [benchmark] [query]
$0 compare <branch1> <branch2>
$0 compare_detail <branch1> <branch2>
$0 venv

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Examples:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Create the datasets for all benchmarks in $DATA_DIR
./bench.sh data

# Run the 'tpch' benchmark on the datafusion checkout in /source/datafusion
DATAFUSION_DIR=/source/datafusion ./bench.sh run tpch

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Commands
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
data:            Generates or downloads data needed for benchmarking
run:             Runs the named benchmark
compare:         Compares fastest results from benchmark runs
compare_detail:  Compares minimum, average (±stddev), and maximum results from benchmark runs
venv:            Creates new venv (unless already exists) and installs compare's requirements into it

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Benchmarks
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Run all of the following benchmarks
all(default): Data/Run/Compare for all benchmarks

# TPC-H Benchmarks
tpch:                   TPCH inspired benchmark on Scale Factor (SF) 1 (~1GB), single parquet file per table, hash join
tpch_csv:               TPCH inspired benchmark on Scale Factor (SF) 1 (~1GB), single csv file per table, hash join
tpch_mem:               TPCH inspired benchmark on Scale Factor (SF) 1 (~1GB), query from memory
tpch10:                 TPCH inspired benchmark on Scale Factor (SF) 10 (~10GB), single parquet file per table, hash join
tpch_csv10:             TPCH inspired benchmark on Scale Factor (SF) 10 (~10GB), single csv file per table, hash join
tpch_mem10:             TPCH inspired benchmark on Scale Factor (SF) 10 (~10GB), query from memory

# TPC-DS Benchmarks
tpcds:                  TPCDS inspired benchmark on Scale Factor (SF) 1 (~1GB), single parquet file per table, hash join

# Extended TPC-H Benchmarks
sort_tpch:              Benchmark of sorting speed for end-to-end sort queries on TPC-H dataset (SF=1)
sort_tpch10:            Benchmark of sorting speed for end-to-end sort queries on TPC-H dataset (SF=10)
topk_tpch:              Benchmark of top-k (sorting with limit) queries on TPC-H dataset (SF=1)
external_aggr:          External aggregation benchmark on TPC-H dataset (SF=1)

# ClickBench Benchmarks
clickbench_1:           ClickBench queries against a single parquet file
clickbench_partitioned: ClickBench queries against partitioned (100 files) parquet
clickbench_pushdown:    ClickBench queries against partitioned (100 files) parquet w/ filter_pushdown enabled
clickbench_extended:    ClickBench \"inspired\" queries against a single parquet (DataFusion specific)

# Sorted Data Benchmarks (ORDER BY Optimization)
clickbench_sorted:     ClickBench queries on pre-sorted data using prefer_existing_sort (tests sort elimination optimization)

# H2O.ai Benchmarks (Group By, Join, Window)
h2o_small:                      h2oai benchmark with small dataset (1e7 rows) for groupby,  default file format is csv
h2o_medium:                     h2oai benchmark with medium dataset (1e8 rows) for groupby, default file format is csv
h2o_big:                        h2oai benchmark with large dataset (1e9 rows) for groupby,  default file format is csv
h2o_small_join:                 h2oai benchmark with small dataset (1e7 rows) for join,  default file format is csv
h2o_medium_join:                h2oai benchmark with medium dataset (1e8 rows) for join, default file format is csv
h2o_big_join:                   h2oai benchmark with large dataset (1e9 rows) for join,  default file format is csv
h2o_small_window:               Extended h2oai benchmark with small dataset (1e7 rows) for window,  default file format is csv
h2o_medium_window:              Extended h2oai benchmark with medium dataset (1e8 rows) for window, default file format is csv
h2o_big_window:                 Extended h2oai benchmark with large dataset (1e9 rows) for window,  default file format is csv
h2o_small_parquet:              h2oai benchmark with small dataset (1e7 rows) for groupby,  file format is parquet
h2o_medium_parquet:             h2oai benchmark with medium dataset (1e8 rows) for groupby, file format is parquet
h2o_big_parquet:                h2oai benchmark with large dataset (1e9 rows) for groupby,  file format is parquet
h2o_small_join_parquet:         h2oai benchmark with small dataset (1e7 rows) for join,  file format is parquet
h2o_medium_join_parquet:        h2oai benchmark with medium dataset (1e8 rows) for join, file format is parquet
h2o_big_join_parquet:           h2oai benchmark with large dataset (1e9 rows) for join,  file format is parquet
h2o_small_window_parquet:       Extended h2oai benchmark with small dataset (1e7 rows) for window,  file format is parquet
h2o_medium_window_parquet:      Extended h2oai benchmark with medium dataset (1e8 rows) for window, file format is parquet
h2o_big_window_parquet:         Extended h2oai benchmark with large dataset (1e9 rows) for window,  file format is parquet

# Join Order Benchmark (IMDB)
imdb:                   Join Order Benchmark (JOB) using the IMDB dataset converted to parquet

# Micro-Benchmarks (specific operators and features)
cancellation:           How long cancelling a query takes
nlj:                    Benchmark for simple nested loop joins, testing various join scenarios
hj:                     Benchmark for simple hash joins, testing various join scenarios
smj:                    Benchmark for simple sort merge joins, testing various join scenarios
compile_profile:        Compile and execute TPC-H across selected Cargo profiles, reporting timing and binary size


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Supported Configuration (Environment Variables)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA_DIR            directory to store datasets
CARGO_COMMAND       command that runs the benchmark binary
DATAFUSION_DIR      directory to use (default $DATAFUSION_DIR)
RESULTS_NAME        folder where the benchmark files are stored
PREFER_HASH_JOIN    Prefer hash join algorithm (default true)
VENV_PATH           Python venv to use for compare and venv commands (default ./venv, override by <your-venv>/bin/activate)
DATAFUSION_*        Set the given datafusion configuration
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
        -*)
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
            echo "DataFusion Benchmark Runner and Data Generator"
            echo "COMMAND: ${COMMAND}"
            echo "BENCHMARK: ${BENCHMARK}"
            echo "DATA_DIR: ${DATA_DIR}"
            echo "CARGO_COMMAND: ${CARGO_COMMAND}"
            echo "PREFER_HASH_JOIN: ${PREFER_HASH_JOIN}"
            echo "***************************"
            case "$BENCHMARK" in
                all)
                    data_tpch "1" "parquet"
                    data_tpch "10" "parquet"
                    data_h2o "SMALL"
                    data_h2o "MEDIUM"
                    data_h2o "BIG"
                    data_h2o_join "SMALL"
                    data_h2o_join "MEDIUM"
                    data_h2o_join "BIG"
                    data_clickbench_1
                    data_clickbench_partitioned
                    data_imdb
                    # nlj uses range() function, no data generation needed
                    ;;
                tpch)
                    data_tpch "1" "parquet"
                    ;;
                tpch_mem)
                    data_tpch "1" "parquet"
                    ;;
                tpch_csv)
                    data_tpch "1" "csv"
                    ;;
                tpch10)
                    data_tpch "10" "parquet"
                    ;;
                tpch_mem10)
                    data_tpch "10" "parquet"
                    ;;
                tpch_csv10)
                    data_tpch "10" "csv"
                    ;;
                tpcds)
                    data_tpcds
                    ;;
                clickbench_1)
                    data_clickbench_1
                    ;;
                clickbench_partitioned)
                    data_clickbench_partitioned
                    ;;
                clickbench_pushdown)
                    data_clickbench_partitioned # same data as clickbench_partitioned
                    ;;
                clickbench_extended)
                    data_clickbench_1
                    ;;
                imdb)
                    data_imdb
                    ;;
                h2o_small)
                    data_h2o "SMALL" "CSV"
                    ;;
                h2o_medium)
                    data_h2o "MEDIUM" "CSV"
                    ;;
                h2o_big)
                    data_h2o "BIG" "CSV"
                    ;;
                h2o_small_join)
                    data_h2o_join "SMALL" "CSV"
                    ;;
                h2o_medium_join)
                    data_h2o_join "MEDIUM" "CSV"
                    ;;
                h2o_big_join)
                    data_h2o_join "BIG" "CSV"
                    ;;
                # h2o window benchmark uses the same data as the h2o join
                h2o_small_window)
                    data_h2o_join "SMALL" "CSV"
                    ;;
                h2o_medium_window)
                    data_h2o_join "MEDIUM" "CSV"
                    ;;
                h2o_big_window)
                    data_h2o_join "BIG" "CSV"
                    ;;
                h2o_small_parquet)
                    data_h2o "SMALL" "PARQUET"
                    ;;
                h2o_medium_parquet)
                    data_h2o "MEDIUM" "PARQUET"
                    ;;
                h2o_big_parquet)
                    data_h2o "BIG" "PARQUET"
                    ;;
                h2o_small_join_parquet)
                    data_h2o_join "SMALL" "PARQUET"
                    ;;
                h2o_medium_join_parquet)
                    data_h2o_join "MEDIUM" "PARQUET"
                    ;;
                h2o_big_join_parquet)
                    data_h2o_join "BIG" "PARQUET"
                    ;;
                # h2o window benchmark uses the same data as the h2o join
                h2o_small_window_parquet)
                    data_h2o_join "SMALL" "PARQUET"
                    ;;
                h2o_medium_window_parquet)
                    data_h2o_join "MEDIUM" "PARQUET"
                    ;;
                h2o_big_window_parquet)
                    data_h2o_join "BIG" "PARQUET"
                    ;;
                external_aggr)
                    # same data as for tpch
                    data_tpch "1" "parquet"
                    ;;
                sort_tpch)
                    # same data as for tpch
                    data_tpch "1" "parquet"
                    ;;
                sort_tpch10)
                    # same data as for tpch10
                    data_tpch "10" "parquet"
                    ;;
                topk_tpch)
                    # same data as for tpch
                    data_tpch "1" "parquet"
                    ;;
                nlj)
                    # nlj uses range() function, no data generation needed
                    echo "NLJ benchmark does not require data generation"
                    ;;
                hj)
                    # hj uses range() function, no data generation needed
                    echo "HJ benchmark does not require data generation"
                    ;;
                smj)
                    # smj uses range() function, no data generation needed
                    echo "SMJ benchmark does not require data generation"
                    ;;
                compile_profile)
                    data_tpch "1" "parquet"
                    ;;
                clickbench_sorted)
                    clickbench_sorted
                    ;;
                *)
                    echo "Error: unknown benchmark '$BENCHMARK' for data generation"
                    usage
                    ;;
            esac
            ;;
        run)
            # Parse positional parameters
            BENCHMARK=${ARG2:-"${BENCHMARK}"}
            EXTRA_ARGS=("${POSITIONAL_ARGS[@]:2}")
            PROFILE_ARGS=()
            QUERY=""
            QUERY_ARG=""
            if [ "$BENCHMARK" = "compile_profile" ]; then
                PROFILE_ARGS=("${EXTRA_ARGS[@]}")
            else
                QUERY=${EXTRA_ARGS[0]}
                if [ -n "$QUERY" ]; then
                    QUERY_ARG="--query ${QUERY}"
                fi
            fi
            BRANCH_NAME=$(cd "${DATAFUSION_DIR}" && git rev-parse --abbrev-ref HEAD)
            BRANCH_NAME=${BRANCH_NAME//\//_} # mind blowing syntax to replace / with _
            RESULTS_NAME=${RESULTS_NAME:-"${BRANCH_NAME}"}
            RESULTS_DIR=${RESULTS_DIR:-"$SCRIPT_DIR/results/$RESULTS_NAME"}

            echo "***************************"
            echo "DataFusion Benchmark Script"
            echo "COMMAND: ${COMMAND}"
            echo "BENCHMARK: ${BENCHMARK}"
            if [ "$BENCHMARK" = "compile_profile" ]; then
                echo "PROFILES: ${PROFILE_ARGS[*]:-All}"
            else
                echo "QUERY: ${QUERY:-All}"
            fi
            echo "DATAFUSION_DIR: ${DATAFUSION_DIR}"
            echo "BRANCH_NAME: ${BRANCH_NAME}"
            echo "DATA_DIR: ${DATA_DIR}"
            echo "RESULTS_DIR: ${RESULTS_DIR}"
            echo "CARGO_COMMAND: ${CARGO_COMMAND}"
            echo "PREFER_HASH_JOIN: ${PREFER_HASH_JOIN}"
            echo "***************************"

            # navigate to the appropriate directory
            pushd "${DATAFUSION_DIR}/benchmarks" > /dev/null
            mkdir -p "${RESULTS_DIR}"
            mkdir -p "${DATA_DIR}"
            case "$BENCHMARK" in
                all)
                    run_tpch "1" "parquet"
                    run_tpch "1" "csv"
                    run_tpch_mem "1"
                    run_tpch "10" "parquet"
                    run_tpch "10" "csv"
                    run_tpch_mem "10"
                    run_cancellation
                    run_clickbench_1
                    run_clickbench_partitioned
                    run_clickbench_pushdown
                    run_clickbench_extended
                    run_h2o "SMALL" "PARQUET" "groupby"
                    run_h2o "MEDIUM" "PARQUET" "groupby"
                    run_h2o "BIG" "PARQUET" "groupby"
                    run_h2o_join "SMALL" "PARQUET" "join"
                    run_h2o_join "MEDIUM" "PARQUET" "join"
                    run_h2o_join "BIG" "PARQUET" "join"
                    run_imdb
                    run_external_aggr
                    run_nlj
                    run_hj
                    run_tpcds
                    run_smj
                    ;;
                tpch)
                    run_tpch "1" "parquet"
                    ;;
                tpch_csv)
                    run_tpch "1" "csv"
                    ;;
                tpch_mem)
                    run_tpch_mem "1"
                    ;;
                tpch10)
                    run_tpch "10" "parquet"
                    ;;
                tpch_csv10)
                    run_tpch "10" "csv"
                    ;;
                tpch_mem10)
                    run_tpch_mem "10"
                    ;;
                tpcds)
                    run_tpcds
                    ;;
                cancellation)
                    run_cancellation
                    ;;
                clickbench_1)
                    run_clickbench_1
                    ;;
                clickbench_partitioned)
                    run_clickbench_partitioned
                    ;;
                clickbench_pushdown)
                    run_clickbench_pushdown
                    ;;
                clickbench_extended)
                    run_clickbench_extended
                    ;;
                imdb)
                    run_imdb
                    ;;
                h2o_small)
                    run_h2o "SMALL" "CSV" "groupby"
                    ;;
                h2o_medium)
                    run_h2o "MEDIUM" "CSV" "groupby"
                    ;;
                h2o_big)
                    run_h2o "BIG" "CSV" "groupby"
                    ;;
                h2o_small_join)
                    run_h2o_join "SMALL" "CSV" "join"
                    ;;
                h2o_medium_join)
                    run_h2o_join "MEDIUM" "CSV" "join"
                    ;;
                h2o_big_join)
                    run_h2o_join "BIG" "CSV" "join"
                    ;;
                h2o_small_window)
                    run_h2o_window "SMALL" "CSV" "window"
                    ;;
                h2o_medium_window)
                    run_h2o_window "MEDIUM" "CSV" "window"
                    ;;
                h2o_big_window)
                    run_h2o_window "BIG" "CSV" "window"
                    ;;
                h2o_small_parquet)
                    run_h2o "SMALL" "PARQUET"
                    ;;
                h2o_medium_parquet)
                    run_h2o "MEDIUM" "PARQUET"
                    ;;
                h2o_big_parquet)
                    run_h2o "BIG" "PARQUET"
                    ;;
                h2o_small_join_parquet)
                    run_h2o_join "SMALL" "PARQUET"
                    ;;
                h2o_medium_join_parquet)
                    run_h2o_join "MEDIUM" "PARQUET"
                    ;;
                h2o_big_join_parquet)
                    run_h2o_join "BIG" "PARQUET"
                    ;;
                # h2o window benchmark uses the same data as the h2o join
                h2o_small_window_parquet)
                    run_h2o_window "SMALL" "PARQUET"
                    ;;
                h2o_medium_window_parquet)
                    run_h2o_window "MEDIUM" "PARQUET"
                    ;;
                h2o_big_window_parquet)
                    run_h2o_window "BIG" "PARQUET"
                    ;;
                external_aggr)
                    run_external_aggr
                    ;;
                sort_tpch)
                    run_sort_tpch "1"
                    ;;
                sort_tpch10)
                    run_sort_tpch "10"
                    ;;
                topk_tpch)
                    run_topk_tpch
                    ;;
                nlj)
                    run_nlj
                    ;;
                hj)
                    run_hj
                    ;;
                smj)
                    run_smj
                    ;;
                compile_profile)
                    run_compile_profile "${PROFILE_ARGS[@]}"
                    ;;
                clickbench_sorted)
                    run_clickbench_sorted
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
            compare_benchmarks "$ARG2" "$ARG3"
            ;;
        compare_detail)
            compare_benchmarks "$ARG2" "$ARG3" "--detailed"
            ;;
        venv)
            setup_venv
            ;;
        "")
            usage
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
# call like: data_tpch($scale_factor, format)
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
    FORMAT=$2
    if [ -z "$FORMAT" ] ; then
        echo "Internal error: Format not specified"
        exit 1
    fi

    TPCH_DIR="${DATA_DIR}/tpch_sf${SCALE_FACTOR}"
    echo "Creating tpch $FORMAT dataset at Scale Factor ${SCALE_FACTOR} in ${TPCH_DIR}..."

    # Ensure the target data directory exists
    mkdir -p "${TPCH_DIR}"

    # check if tpchgen-cli is installed
    if ! command -v tpchgen-cli &> /dev/null
    then
        echo "tpchgen-cli could not be found, please install it via 'cargo install tpchgen-cli'"
        exit 1
    fi

    # Copy expected answers into the ./data/answers directory if it does not already exist
    FILE="${TPCH_DIR}/answers/q1.out"
    if test -f "${FILE}"; then
        echo " Expected answers exist (${FILE} exists)."
    else
        echo " Copying answers to ${TPCH_DIR}/answers"
        mkdir -p "${TPCH_DIR}/answers"
        docker run -v "${TPCH_DIR}":/data -it --entrypoint /bin/bash --rm ghcr.io/scalytics/tpch-docker:main  -c "cp -f /opt/tpch/2.18.0_rc2/dbgen/answers/* /data/answers/"
    fi

    if [ "$FORMAT" = "parquet" ]; then
      # Create 'parquet' files, one directory per file
      FILE="${TPCH_DIR}/supplier"
      if test -d "${FILE}"; then
          echo " parquet files exist ($FILE exists)."
      else
          echo " creating parquet files using tpchgen-cli ..."
          tpchgen-cli --scale-factor "${SCALE_FACTOR}" --format parquet --parquet-compression='ZSTD(1)' --parts=1 --output-dir "${TPCH_DIR}"
      fi
      return
    fi

    # Create 'csv' files, one directory per file
    if [ "$FORMAT" = "csv" ]; then
      FILE="${TPCH_DIR}/csv/supplier"
      if test -d "${FILE}"; then
          echo " csv files exist ($FILE exists)."
      else
          echo " creating csv files using tpchgen-cli binary ..."
          tpchgen-cli --scale-factor "${SCALE_FACTOR}" --format csv --parts=1 --output-dir "${TPCH_DIR}/csv"
      fi
      return
    fi

    echo "Error: unknown format '$FORMAT' for tpch data generation, expected 'parquet' or 'csv'"
    exit 1
}

# Points to TPCDS data generation instructions
data_tpcds() {
    TPCDS_DIR="${DATA_DIR}/tpcds_sf1"

    # Check if `web_site.parquet` exists in the TPCDS data directory to verify data presence
    echo "Checking TPC-DS data directory: ${TPCDS_DIR}"
    if [ ! -f "${TPCDS_DIR}/web_site.parquet" ]; then
        mkdir -p "${TPCDS_DIR}"
        # Download the DataFusion benchmarks repository zip if it is not already downloaded
        if [ ! -f "${DATA_DIR}/datafusion-benchmarks.zip" ]; then
          echo "Downloading DataFusion benchmarks repository zip to: ${DATA_DIR}/datafusion-benchmarks.zip"
          wget -O "${DATA_DIR}/datafusion-benchmarks.zip" https://github.com/apache/datafusion-benchmarks/archive/refs/heads/main.zip
        fi
        echo "Extracting TPC-DS parquet data to ${TPCDS_DIR}..."
        unzip -o -j -d "${TPCDS_DIR}" "${DATA_DIR}/datafusion-benchmarks.zip" datafusion-benchmarks-main/tpcds/data/sf1/*
        echo "TPC-DS data extracted."
    fi
    echo "Done."
}

# Runs the tpch benchmark
run_tpch() {
    SCALE_FACTOR=$1
    if [ -z "$SCALE_FACTOR" ] ; then
        echo "Internal error: Scale factor not specified"
        exit 1
    fi
    TPCH_DIR="${DATA_DIR}/tpch_sf${SCALE_FACTOR}"

    RESULTS_FILE="${RESULTS_DIR}/tpch_sf${SCALE_FACTOR}.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running tpch benchmark..."

    FORMAT=$2
    debug_run $CARGO_COMMAND --bin dfbench -- tpch --iterations 5 --path "${TPCH_DIR}" --prefer_hash_join "${PREFER_HASH_JOIN}" --format ${FORMAT} -o "${RESULTS_FILE}" ${QUERY_ARG}
}

# Runs the tpch in memory (needs tpch parquet data)
run_tpch_mem() {
    SCALE_FACTOR=$1
    if [ -z "$SCALE_FACTOR" ] ; then
        echo "Internal error: Scale factor not specified"
        exit 1
    fi
    TPCH_DIR="${DATA_DIR}/tpch_sf${SCALE_FACTOR}"

    RESULTS_FILE="${RESULTS_DIR}/tpch_mem_sf${SCALE_FACTOR}.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running tpch_mem benchmark..."
    # -m means in memory
    debug_run $CARGO_COMMAND --bin dfbench -- tpch --iterations 5 --path "${TPCH_DIR}" --prefer_hash_join "${PREFER_HASH_JOIN}" -m --format parquet -o "${RESULTS_FILE}" ${QUERY_ARG}
}

# Runs the tpcds benchmark
run_tpcds() {
    TPCDS_DIR="${DATA_DIR}/tpcds_sf1"

    # Check if TPCDS data directory and representative file exists
    if [ ! -f "${TPCDS_DIR}/web_site.parquet" ]; then
        echo "" >&2
        echo "Please prepare TPC-DS data first by following instructions:" >&2
        echo "  ./bench.sh data tpcds" >&2
        echo "" >&2
        exit 1
    fi

    RESULTS_FILE="${RESULTS_DIR}/tpcds_sf1.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running tpcds benchmark..."

    debug_run $CARGO_COMMAND --bin dfbench -- tpcds --iterations 5 --path "${TPCDS_DIR}" --query_path "../datafusion/core/tests/tpc-ds" --prefer_hash_join "${PREFER_HASH_JOIN}" -o "${RESULTS_FILE}" ${QUERY_ARG}
}

# Runs the compile profile benchmark helper
run_compile_profile() {
    local profiles=("$@")
    local runner="${SCRIPT_DIR}/compile_profile.py"
    local data_path="${DATA_DIR}/tpch_sf1"

    echo "Running compile profile benchmark..."
    local cmd=(python3 "${runner}" --data "${data_path}")
    if [ ${#profiles[@]} -gt 0 ]; then
        cmd+=(--profiles "${profiles[@]}")
    fi
    debug_run "${cmd[@]}"
}

# Runs the cancellation benchmark
run_cancellation() {
    RESULTS_FILE="${RESULTS_DIR}/cancellation.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running cancellation benchmark..."
    debug_run $CARGO_COMMAND --bin dfbench -- cancellation --iterations 5 --path "${DATA_DIR}/cancellation" -o "${RESULTS_FILE}"
}


# Downloads the single file hits.parquet ClickBench datasets from
# https://github.com/ClickHouse/ClickBench/tree/main#data-loading
#
# Creates data in $DATA_DIR/hits.parquet
data_clickbench_1() {
    pushd "${DATA_DIR}" > /dev/null

    # Avoid downloading if it already exists and is the right size
    OUTPUT_SIZE=$(wc -c hits.parquet  2>/dev/null  | awk '{print $1}' || true)
    echo -n "Checking hits.parquet..."
    if test "${OUTPUT_SIZE}" = "14779976446"; then
        echo -n "... found ${OUTPUT_SIZE} bytes ..."
    else
        URL="https://datasets.clickhouse.com/hits_compatible/hits.parquet"
        echo -n "... downloading ${URL} (14GB) ... "
        wget --continue ${URL}
    fi
    echo " Done"
    popd > /dev/null
}

# Downloads the 100 file partitioned ClickBench datasets from
# https://github.com/ClickHouse/ClickBench/tree/main#data-loading
#
# Creates data in $DATA_DIR/hits_partitioned
data_clickbench_partitioned() {
    MAX_CONCURRENT_DOWNLOADS=10

    mkdir -p "${DATA_DIR}/hits_partitioned"
    pushd "${DATA_DIR}/hits_partitioned" > /dev/null

    echo -n "Checking hits_partitioned..."
    OUTPUT_SIZE=$(wc -c -- * 2>/dev/null | tail -n 1  | awk '{print $1}' || true)
    if test "${OUTPUT_SIZE}" = "14737666736"; then
        echo -n "... found ${OUTPUT_SIZE} bytes ..."
    else
        echo -n " downloading with ${MAX_CONCURRENT_DOWNLOADS} parallel workers"
        seq 0 99 | xargs -P${MAX_CONCURRENT_DOWNLOADS} -I{} bash -c 'wget -q --continue https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet && echo -n "."'
    fi

    echo " Done"
    popd > /dev/null
}


# Runs the clickbench benchmark with a single large parquet file
run_clickbench_1() {
    RESULTS_FILE="${RESULTS_DIR}/clickbench_1.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running clickbench (1 file) benchmark..."
    debug_run $CARGO_COMMAND --bin dfbench -- clickbench  --iterations 5 --path "${DATA_DIR}/hits.parquet"  --queries-path "${SCRIPT_DIR}/queries/clickbench/queries" -o "${RESULTS_FILE}" ${QUERY_ARG}
}

 # Runs the clickbench benchmark with the partitioned parquet dataset (100 files)
run_clickbench_partitioned() {
    RESULTS_FILE="${RESULTS_DIR}/clickbench_partitioned.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running clickbench (partitioned, 100 files) benchmark..."
    debug_run $CARGO_COMMAND --bin dfbench -- clickbench  --iterations 5 --path "${DATA_DIR}/hits_partitioned" --queries-path "${SCRIPT_DIR}/queries/clickbench/queries" -o "${RESULTS_FILE}" ${QUERY_ARG}
}


 # Runs the clickbench benchmark with the partitioned parquet files and filter_pushdown enabled
run_clickbench_pushdown() {
    RESULTS_FILE="${RESULTS_DIR}/clickbench_pushdown.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running clickbench (partitioned, 100 files) benchmark with pushdown_filters=true, reorder_filters=true..."
    debug_run $CARGO_COMMAND --bin dfbench -- clickbench --pushdown --iterations 5 --path "${DATA_DIR}/hits_partitioned" --queries-path "${SCRIPT_DIR}/queries/clickbench/queries" -o "${RESULTS_FILE}" ${QUERY_ARG}
}


# Runs the clickbench "extended" benchmark with a single large parquet file
run_clickbench_extended() {
    RESULTS_FILE="${RESULTS_DIR}/clickbench_extended.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running clickbench (1 file) extended benchmark..."
    debug_run $CARGO_COMMAND --bin dfbench -- clickbench  --iterations 5 --path "${DATA_DIR}/hits.parquet" --queries-path "${SCRIPT_DIR}/queries/clickbench/extended" -o "${RESULTS_FILE}" ${QUERY_ARG}
}

# Downloads the csv.gz files IMDB datasets from Peter Boncz's homepage(one of the JOB paper authors)
# https://event.cwi.nl/da/job/imdb.tgz
data_imdb() {
    local imdb_dir="${DATA_DIR}/imdb"
    local imdb_temp_gz="${imdb_dir}/imdb.tgz"
    local imdb_url="https://event.cwi.nl/da/job/imdb.tgz"

   # imdb has 21 files, we just separate them into 3 groups for better readability
    local first_required_files=(
        "aka_name.parquet"
        "aka_title.parquet"
        "cast_info.parquet"
        "char_name.parquet"
        "comp_cast_type.parquet"
        "company_name.parquet"
        "company_type.parquet"
    )

    local second_required_files=(
        "complete_cast.parquet"
        "info_type.parquet"
        "keyword.parquet"
        "kind_type.parquet"
        "link_type.parquet"
        "movie_companies.parquet"
        "movie_info.parquet"
    )

    local third_required_files=(
        "movie_info_idx.parquet"
        "movie_keyword.parquet"
        "movie_link.parquet"
        "name.parquet"
        "person_info.parquet"
        "role_type.parquet"
        "title.parquet"
    )

    # Combine the three arrays into one
    local required_files=("${first_required_files[@]}" "${second_required_files[@]}" "${third_required_files[@]}")
    local convert_needed=false

    # Create directory if it doesn't exist
    mkdir -p "${imdb_dir}"

    # Check if required files exist
    for file in "${required_files[@]}"; do
        if [ ! -f "${imdb_dir}/${file}" ]; then
            convert_needed=true
            break
        fi
    done

    if [ "$convert_needed" = true ]; then
        # Expected size of the dataset
        expected_size="1263193115" # 1.18 GB

        echo -n "Looking for imdb.tgz... "
        if [ -f "${imdb_temp_gz}" ]; then
            echo "found"
            echo -n "Checking size... "
            OUTPUT_SIZE=$(wc -c "${imdb_temp_gz}" 2>/dev/null | awk '{print $1}' || true)

            #Checking the size of the existing file
            if [ "${OUTPUT_SIZE}" = "${expected_size}" ]; then
                # Existing file is of the expected size, no need for download
                echo "OK ${OUTPUT_SIZE}"
            else
                # Existing file is partially installed, remove it and initiate a new download
                echo "MISMATCH"
                echo "Size less than expected: ${OUTPUT_SIZE} found, ${expected_size} required"
                echo "Downloading IMDB dataset..."
                rm -f "${imdb_temp_gz}"

                # Download the dataset
                curl -o "${imdb_temp_gz}" "${imdb_url}"

                # Size check of the installed file
                DOWNLOADED_SIZE=$(wc -c "${imdb_temp_gz}" | awk '{print $1}')
                if [ "${DOWNLOADED_SIZE}" != "${expected_size}" ]; then
                    echo "Error: Download size mismatch"
                    echo "Expected: ${expected_size}"
                    echo "Got: ${DOWNLADED_SIZE}"
                    echo "Please re-initiate the download"
                    return 1
                fi
            fi
        else
            # No existing file found, initiate a new download
            echo "not found"
            echo "Downloading IMDB dataset ${expected_size} expected)..."
            # Download the dataset
            curl -o "${imdb_temp_gz}" "${imdb_url}"
        fi

        # Extract the dataset
        tar -xzvf "${imdb_temp_gz}" -C "${imdb_dir}"
        $CARGO_COMMAND --bin imdb -- convert --input ${imdb_dir} --output ${imdb_dir} --format parquet
        echo "IMDB dataset downloaded and extracted."

    else
        echo "IMDB dataset already exists and contains required parquet files."
    fi
}

# Runs the imdb benchmark
run_imdb() {
    IMDB_DIR="${DATA_DIR}/imdb"

    RESULTS_FILE="${RESULTS_DIR}/imdb.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running imdb benchmark..."
    debug_run $CARGO_COMMAND --bin imdb -- benchmark datafusion --iterations 5 --path "${IMDB_DIR}" --prefer_hash_join "${PREFER_HASH_JOIN}" --format parquet -o "${RESULTS_FILE}" ${QUERY_ARG}
}

data_h2o() {
    # Default values for size and data format
    SIZE=${1:-"SMALL"}
    DATA_FORMAT=${2:-"CSV"}

    # Function to compare Python versions
    version_ge() {
        [ "$(printf '%s\n' "$1" "$2" | sort -V | head -n1)" = "$2" ]
    }

    export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

    # Find the highest available Python version (3.10 or higher)
    REQUIRED_VERSION="3.10"
    PYTHON_CMD=$(command -v python3 || true)

    if [ -n "$PYTHON_CMD" ]; then
        PYTHON_VERSION=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        if version_ge "$PYTHON_VERSION" "$REQUIRED_VERSION"; then
            echo "Found Python version $PYTHON_VERSION, which is suitable."
        else
            echo "Python version $PYTHON_VERSION found, but version $REQUIRED_VERSION or higher is required."
            PYTHON_CMD=""
        fi
    fi

   # Search for suitable Python versions if the default is unsuitable
   if [ -z "$PYTHON_CMD" ]; then
       # Loop through all available Python3 commands on the system
       for CMD in $(compgen -c | grep -E '^python3(\.[0-9]+)?$'); do
           if command -v "$CMD" &> /dev/null; then
               PYTHON_VERSION=$($CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
               if version_ge "$PYTHON_VERSION" "$REQUIRED_VERSION"; then
                   PYTHON_CMD="$CMD"
                   echo "Found suitable Python version: $PYTHON_VERSION ($CMD)"
                   break
               fi
           fi
       done
   fi

    # If no suitable Python version found, exit with an error
    if [ -z "$PYTHON_CMD" ]; then
        echo "Python 3.10 or higher is required. Please install it."
        return 1
    fi

    echo "Using Python command: $PYTHON_CMD"

    # Install falsa and other dependencies
    echo "Installing falsa..."

    # Set virtual environment directory
    VIRTUAL_ENV="${PWD}/venv"

    # Create a virtual environment using the detected Python command
    $PYTHON_CMD -m venv "$VIRTUAL_ENV"

    # Activate the virtual environment and install dependencies
    source "$VIRTUAL_ENV/bin/activate"

    # Ensure 'falsa' is installed (avoid unnecessary reinstall)
    pip install --quiet --upgrade falsa

    # Create directory if it doesn't exist
    H2O_DIR="${DATA_DIR}/h2o"
    mkdir -p "${H2O_DIR}"

    # Generate h2o test data
    echo "Generating h2o test data in ${H2O_DIR} with size=${SIZE} and format=${DATA_FORMAT}"
    falsa groupby --path-prefix="${H2O_DIR}" --size "${SIZE}" --data-format "${DATA_FORMAT}"

    # Deactivate virtual environment after completion
    deactivate
}

data_h2o_join() {
    # Default values for size and data format
    SIZE=${1:-"SMALL"}
    DATA_FORMAT=${2:-"CSV"}

    # Function to compare Python versions
    version_ge() {
        [ "$(printf '%s\n' "$1" "$2" | sort -V | head -n1)" = "$2" ]
    }

    export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

    # Find the highest available Python version (3.10 or higher)
    REQUIRED_VERSION="3.10"
    PYTHON_CMD=$(command -v python3 || true)

    if [ -n "$PYTHON_CMD" ]; then
        PYTHON_VERSION=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        if version_ge "$PYTHON_VERSION" "$REQUIRED_VERSION"; then
            echo "Found Python version $PYTHON_VERSION, which is suitable."
        else
            echo "Python version $PYTHON_VERSION found, but version $REQUIRED_VERSION or higher is required."
            PYTHON_CMD=""
        fi
    fi

   # Search for suitable Python versions if the default is unsuitable
   if [ -z "$PYTHON_CMD" ]; then
       # Loop through all available Python3 commands on the system
       for CMD in $(compgen -c | grep -E '^python3(\.[0-9]+)?$'); do
           if command -v "$CMD" &> /dev/null; then
               PYTHON_VERSION=$($CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
               if version_ge "$PYTHON_VERSION" "$REQUIRED_VERSION"; then
                   PYTHON_CMD="$CMD"
                   echo "Found suitable Python version: $PYTHON_VERSION ($CMD)"
                   break
               fi
           fi
       done
   fi

    # If no suitable Python version found, exit with an error
    if [ -z "$PYTHON_CMD" ]; then
        echo "Python 3.10 or higher is required. Please install it."
        return 1
    fi

    echo "Using Python command: $PYTHON_CMD"

    # Install falsa and other dependencies
    echo "Installing falsa..."

    # Set virtual environment directory
    VIRTUAL_ENV="${PWD}/venv"

    # Create a virtual environment using the detected Python command
    $PYTHON_CMD -m venv "$VIRTUAL_ENV"

    # Activate the virtual environment and install dependencies
    source "$VIRTUAL_ENV/bin/activate"

    # Ensure 'falsa' is installed (avoid unnecessary reinstall)
    pip install --quiet --upgrade falsa

    # Create directory if it doesn't exist
    H2O_DIR="${DATA_DIR}/h2o"
    mkdir -p "${H2O_DIR}"

    # Generate h2o test data
    echo "Generating h2o test data in ${H2O_DIR} with size=${SIZE} and format=${DATA_FORMAT}"
    falsa join --path-prefix="${H2O_DIR}" --size "${SIZE}" --data-format "${DATA_FORMAT}"

    # Deactivate virtual environment after completion
    deactivate
}

# Runner for h2o groupby benchmark
run_h2o() {
    # Default values for size and data format
    SIZE=${1:-"SMALL"}
    DATA_FORMAT=${2:-"CSV"}
    DATA_FORMAT=$(echo "$DATA_FORMAT" | tr '[:upper:]' '[:lower:]')
    RUN_Type=${3:-"groupby"}

    # Data directory and results file path
    H2O_DIR="${DATA_DIR}/h2o"
    RESULTS_FILE="${RESULTS_DIR}/h2o.json"

    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running h2o groupby benchmark..."

    # Set the file name based on the size
    case "$SIZE" in
        "SMALL")
            FILE_NAME="G1_1e7_1e7_100_0.${DATA_FORMAT}"  # For small dataset
            ;;
        "MEDIUM")
            FILE_NAME="G1_1e8_1e8_100_0.${DATA_FORMAT}"  # For medium dataset
            ;;
        "BIG")
            FILE_NAME="G1_1e9_1e9_100_0.${DATA_FORMAT}"  # For big dataset
            ;;
        *)
            echo "Invalid size. Valid options are SMALL, MEDIUM, or BIG."
            return 1
            ;;
    esac

     # Set the query file name based on the RUN_Type
    QUERY_FILE="${SCRIPT_DIR}/queries/h2o/${RUN_Type}.sql"

    # Run the benchmark using the dynamically constructed file path and query file
    debug_run $CARGO_COMMAND --bin dfbench -- h2o \
        --iterations 3 \
        --path "${H2O_DIR}/${FILE_NAME}" \
        --queries-path "${QUERY_FILE}" \
        -o "${RESULTS_FILE}" \
         ${QUERY_ARG}
}

# Utility function to run h2o join/window benchmark
h2o_runner() {
    # Default values for size and data format
    SIZE=${1:-"SMALL"}
    DATA_FORMAT=${2:-"CSV"}
    DATA_FORMAT=$(echo "$DATA_FORMAT" | tr '[:upper:]' '[:lower:]')
    RUN_Type=${3:-"join"}

    # Data directory and results file path
    H2O_DIR="${DATA_DIR}/h2o"
    RESULTS_FILE="${RESULTS_DIR}/h2o_${RUN_Type}.json"

    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running h2o ${RUN_Type} benchmark..."

    # Set the file name based on the size
    case "$SIZE" in
        "SMALL")
            X_TABLE_FILE_NAME="J1_1e7_NA_0.${DATA_FORMAT}"
            SMALL_TABLE_FILE_NAME="J1_1e7_1e1_0.${DATA_FORMAT}"
            MEDIUM_TABLE_FILE_NAME="J1_1e7_1e4_0.${DATA_FORMAT}"
            LARGE_TABLE_FILE_NAME="J1_1e7_1e7_NA.${DATA_FORMAT}"
            ;;
        "MEDIUM")
            X_TABLE_FILE_NAME="J1_1e8_NA_0.${DATA_FORMAT}"
            SMALL_TABLE_FILE_NAME="J1_1e8_1e2_0.${DATA_FORMAT}"
            MEDIUM_TABLE_FILE_NAME="J1_1e8_1e5_0.${DATA_FORMAT}"
            LARGE_TABLE_FILE_NAME="J1_1e8_1e8_NA.${DATA_FORMAT}"
            ;;
        "BIG")
            X_TABLE_FILE_NAME="J1_1e9_NA_0.${DATA_FORMAT}"
            SMALL_TABLE_FILE_NAME="J1_1e9_1e3_0.${DATA_FORMAT}"
            MEDIUM_TABLE_FILE_NAME="J1_1e9_1e6_0.${DATA_FORMAT}"
            LARGE_TABLE_FILE_NAME="J1_1e9_1e9_NA.${DATA_FORMAT}"
            ;;
        *)
            echo "Invalid size. Valid options are SMALL, MEDIUM, or BIG."
            return 1
            ;;
    esac

    # Set the query file name based on the RUN_Type
    QUERY_FILE="${SCRIPT_DIR}/queries/h2o/${RUN_Type}.sql"

    debug_run $CARGO_COMMAND --bin dfbench -- h2o \
        --iterations 3 \
        --join-paths "${H2O_DIR}/${X_TABLE_FILE_NAME},${H2O_DIR}/${SMALL_TABLE_FILE_NAME},${H2O_DIR}/${MEDIUM_TABLE_FILE_NAME},${H2O_DIR}/${LARGE_TABLE_FILE_NAME}" \
        --queries-path "${QUERY_FILE}" \
        -o "${RESULTS_FILE}" \
         ${QUERY_ARG}
}

# Runners for h2o join benchmark
run_h2o_join() {
    h2o_runner "$1" "$2" "join"
}

# Runners for h2o join benchmark
run_h2o_window() {
    h2o_runner "$1" "$2" "window"
}

# Runs the external aggregation benchmark
run_external_aggr() {
    # Use TPC-H SF1 dataset
    TPCH_DIR="${DATA_DIR}/tpch_sf1"
    RESULTS_FILE="${RESULTS_DIR}/external_aggr.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running external aggregation benchmark..."

    # Only parquet is supported.
    # Since per-operator memory limit is calculated as (total-memory-limit /
    # number-of-partitions), and by default `--partitions` is set to number of
    # CPU cores, we set a constant number of partitions to prevent this
    # benchmark to fail on some machines.
    debug_run $CARGO_COMMAND --bin external_aggr -- benchmark --partitions 4 --iterations 5 --path "${TPCH_DIR}" -o "${RESULTS_FILE}" ${QUERY_ARG}
}

# Runs the sort integration benchmark
run_sort_tpch() {
    SCALE_FACTOR=$1
    if [ -z "$SCALE_FACTOR" ] ; then
        echo "Internal error: Scale factor not specified"
        exit 1
    fi
    TPCH_DIR="${DATA_DIR}/tpch_sf${SCALE_FACTOR}"
    RESULTS_FILE="${RESULTS_DIR}/sort_tpch${SCALE_FACTOR}.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running sort tpch benchmark..."

    debug_run $CARGO_COMMAND --bin dfbench -- sort-tpch --iterations 5 --path "${TPCH_DIR}" -o "${RESULTS_FILE}" ${QUERY_ARG}
}

# Runs the sort tpch integration benchmark with limit 100 (topk)
run_topk_tpch() {
    TPCH_DIR="${DATA_DIR}/tpch_sf1"
    RESULTS_FILE="${RESULTS_DIR}/run_topk_tpch.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running topk tpch benchmark..."

    $CARGO_COMMAND --bin dfbench -- sort-tpch --iterations 5 --path "${TPCH_DIR}" -o "${RESULTS_FILE}" --limit 100 ${QUERY_ARG}
}

# Runs the nlj benchmark
run_nlj() {
    RESULTS_FILE="${RESULTS_DIR}/nlj.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running nlj benchmark..."
    debug_run $CARGO_COMMAND --bin dfbench -- nlj --iterations 5 -o "${RESULTS_FILE}" ${QUERY_ARG}
}

# Runs the hj benchmark
run_hj() {
    RESULTS_FILE="${RESULTS_DIR}/hj.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running hj benchmark..."
    debug_run $CARGO_COMMAND --bin dfbench -- hj --iterations 5 -o "${RESULTS_FILE}" ${QUERY_ARG}
}

# Runs the smj benchmark
run_smj() {
    RESULTS_FILE="${RESULTS_DIR}/smj.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running smj benchmark..."
    debug_run $CARGO_COMMAND --bin dfbench -- smj --iterations 5 -o "${RESULTS_FILE}" ${QUERY_ARG}
}


compare_benchmarks() {
    BASE_RESULTS_DIR="${SCRIPT_DIR}/results"
    BRANCH1="$1"
    BRANCH2="$2"
    OPTS="$3"

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
    for RESULTS_FILE1 in "${BASE_RESULTS_DIR}/${BRANCH1}"/*.json ; do
	BENCH=$(basename "${RESULTS_FILE1}")
        RESULTS_FILE2="${BASE_RESULTS_DIR}/${BRANCH2}/${BENCH}"
        if test -f "${RESULTS_FILE2}" ; then
            echo "--------------------"
            echo "Benchmark ${BENCH}"
            echo "--------------------"
            PATH=$VIRTUAL_ENV/bin:$PATH python3 "${SCRIPT_DIR}"/compare.py $OPTS "${RESULTS_FILE1}" "${RESULTS_FILE2}"
        else
            echo "Note: Skipping ${RESULTS_FILE1} as ${RESULTS_FILE2} does not exist"
        fi
    done

}

# Creates sorted ClickBench data from hits.parquet (full dataset)
# The data is sorted by EventTime in ascending order
# Uses datafusion-cli to reduce dependencies
clickbench_sorted() {
    SORTED_FILE="${DATA_DIR}/hits_sorted.parquet"
    ORIGINAL_FILE="${DATA_DIR}/hits.parquet"

    # Default memory limit is 12GB, can be overridden with DATAFUSION_MEMORY_GB env var
    MEMORY_LIMIT_GB=${DATAFUSION_MEMORY_GB:-12}

    echo "Creating sorted ClickBench dataset from hits.parquet..."
    echo "Configuration:"
    echo "  Memory limit: ${MEMORY_LIMIT_GB}G"
    echo "  Row group size: 64K rows"
    echo "  Compression: uncompressed"

    if [ ! -f "${ORIGINAL_FILE}" ]; then
        echo "hits.parquet not found. Running data_clickbench_1 first..."
        data_clickbench_1
    fi

    if [ -f "${SORTED_FILE}" ]; then
        echo "Sorted hits.parquet already exists at ${SORTED_FILE}"
        return 0
    fi

    echo "Sorting hits.parquet by EventTime (this may take several minutes)..."

    pushd "${DATAFUSION_DIR}" > /dev/null
    echo "Building datafusion-cli..."
    cargo build --release --bin datafusion-cli
    DATAFUSION_CLI="${DATAFUSION_DIR}/target/release/datafusion-cli"
    popd > /dev/null


    START_TIME=$(date +%s)
    echo "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Using datafusion-cli to create sorted parquet file..."
    "${DATAFUSION_CLI}" << EOF
-- Memory and performance configuration
SET datafusion.runtime.memory_limit = '${MEMORY_LIMIT_GB}G';
SET datafusion.execution.spill_compression = 'uncompressed';
SET datafusion.execution.sort_spill_reservation_bytes = 10485760; -- 10MB
SET datafusion.execution.batch_size = 8192;
SET datafusion.execution.target_partitions = 1;

-- Parquet output configuration
SET datafusion.execution.parquet.max_row_group_size = 65536;
SET datafusion.execution.parquet.compression = 'uncompressed';

-- Execute sort and write
COPY (SELECT * FROM '${ORIGINAL_FILE}' ORDER BY "EventTime")
TO '${SORTED_FILE}'
STORED AS PARQUET;
EOF

    local result=$?

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    echo "End time: $(date '+%Y-%m-%d %H:%M:%S')"

    if [ $result -eq 0 ]; then
        echo "✓ Successfully created sorted ClickBench dataset"

        INPUT_SIZE=$(stat -f%z "${ORIGINAL_FILE}" 2>/dev/null || stat -c%s "${ORIGINAL_FILE}" 2>/dev/null)
        OUTPUT_SIZE=$(stat -f%z "${SORTED_FILE}" 2>/dev/null || stat -c%s "${SORTED_FILE}" 2>/dev/null)
        INPUT_MB=$((INPUT_SIZE / 1024 / 1024))
        OUTPUT_MB=$((OUTPUT_SIZE / 1024 / 1024))

        echo "  Input:  ${INPUT_MB} MB"
        echo "  Output: ${OUTPUT_MB} MB"

        echo ""
        echo "Time Statistics:"
        echo "  Total duration: ${DURATION} seconds ($(printf '%02d:%02d:%02d' $((DURATION/3600)) $((DURATION%3600/60)) $((DURATION%60))))"
        echo "  Throughput: $((INPUT_MB / DURATION)) MB/s"

        return 0
    else
        echo "✗ Error: Failed to create sorted dataset"
        echo "💡 Tip: Try increasing memory with: DATAFUSION_MEMORY_GB=16 ./bench.sh data clickbench_sorted"
        return 1
    fi
}

# Runs the sorted data benchmark with prefer_existing_sort configuration
run_clickbench_sorted() {
    RESULTS_FILE="${RESULTS_DIR}/clickbench_sorted.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running sorted data benchmark with prefer_existing_sort optimization..."

    # Ensure sorted data exists
    clickbench_sorted

    # Run benchmark with prefer_existing_sort configuration
    # This allows DataFusion to optimize away redundant sorts while maintaining parallelism
    debug_run $CARGO_COMMAND --bin dfbench -- clickbench \
        --iterations 5 \
        --path "${DATA_DIR}/hits_sorted.parquet" \
        --queries-path "${SCRIPT_DIR}/queries/clickbench/queries/sorted_data" \
        --sorted-by "EventTime" \
        -c datafusion.optimizer.prefer_existing_sort=true \
        -o "${RESULTS_FILE}" \
        ${QUERY_ARG}
}

setup_venv() {
    python3 -m venv "$VIRTUAL_ENV"
    PATH=$VIRTUAL_ENV/bin:$PATH python3 -m pip install -r requirements.txt
}

# And start the process up
main
