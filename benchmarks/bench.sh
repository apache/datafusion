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
$0 data [benchmark] [query]
$0 run [benchmark]
$0 compare <branch1> <branch2>
$0 venv

**********
Examples:
**********
# Create the datasets for all benchmarks in $DATA_DIR
./bench.sh data

# Run the 'tpch' benchmark on the datafusion checkout in /source/datafusion
DATAFUSION_DIR=/source/datafusion ./bench.sh run tpch

**********
* Commands
**********
data:         Generates or downloads data needed for benchmarking
run:          Runs the named benchmark
compare:      Compares results from benchmark runs
venv:         Creates new venv (unless already exists) and installs compare's requirements into it

**********
* Benchmarks
**********
all(default): Data/Run/Compare for all benchmarks
tpch:                   TPCH inspired benchmark on Scale Factor (SF) 1 (~1GB), single parquet file per table, hash join
tpch_mem:               TPCH inspired benchmark on Scale Factor (SF) 1 (~1GB), query from memory
tpch10:                 TPCH inspired benchmark on Scale Factor (SF) 10 (~10GB), single parquet file per table, hash join
tpch_mem10:             TPCH inspired benchmark on Scale Factor (SF) 10 (~10GB), query from memory
cancellation:           How long cancelling a query takes
parquet:                Benchmark of parquet reader's filtering speed
sort:                   Benchmark of sorting speed
sort_tpch:              Benchmark of sorting speed for end-to-end sort queries on TPCH dataset
clickbench_1:           ClickBench queries against a single parquet file
clickbench_partitioned: ClickBench queries against a partitioned (100 files) parquet
clickbench_extended:    ClickBench \"inspired\" queries against a single parquet (DataFusion specific)
external_aggr:          External aggregation benchmark
h2o_small:              h2oai benchmark with small dataset (1e7 rows) for groupby,  default file format is csv
h2o_medium:             h2oai benchmark with medium dataset (1e8 rows) for groupby, default file format is csv
h2o_big:                h2oai benchmark with large dataset (1e9 rows) for groupby,  default file format is csv
h2o_small_join:         h2oai benchmark with small dataset (1e7 rows) for join,  default file format is csv
h2o_medium_join:        h2oai benchmark with medium dataset (1e8 rows) for join, default file format is csv
h2o_big_join:           h2oai benchmark with large dataset (1e9 rows) for join,  default file format is csv
imdb:                   Join Order Benchmark (JOB) using the IMDB dataset converted to parquet

**********
* Supported Configuration (Environment Variables)
**********
DATA_DIR            directory to store datasets
CARGO_COMMAND       command that runs the benchmark binary
DATAFUSION_DIR      directory to use (default $DATAFUSION_DIR)
RESULTS_NAME        folder where the benchmark files are stored
PREFER_HASH_JOIN    Prefer hash join algorithm (default true)
VENV_PATH           Python venv to use for compare and venv commands (default ./venv, override by <your-venv>/bin/activate)
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
                    data_tpch "1"
                    data_tpch "10"
                    data_h2o "SMALL"
                    data_h2o "MEDIUM"
                    data_h2o "BIG"
                    data_h2o_join "SMALL"
                    data_h2o_join "MEDIUM"
                    data_h2o_join "BIG"
                    data_clickbench_1
                    data_clickbench_partitioned
                    data_imdb
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
                clickbench_1)
                    data_clickbench_1
                    ;;
                clickbench_partitioned)
                    data_clickbench_partitioned
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
                external_aggr)
                    # same data as for tpch
                    data_tpch "1"
                    ;;
                sort_tpch)
                    # same data as for tpch
                    data_tpch "1"
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
            BRANCH_NAME=$(cd "${DATAFUSION_DIR}" && git rev-parse --abbrev-ref HEAD)
            BRANCH_NAME=${BRANCH_NAME//\//_} # mind blowing syntax to replace / with _
            RESULTS_NAME=${RESULTS_NAME:-"${BRANCH_NAME}"}
            RESULTS_DIR=${RESULTS_DIR:-"$SCRIPT_DIR/results/$RESULTS_NAME"}

            echo "***************************"
            echo "DataFusion Benchmark Script"
            echo "COMMAND: ${COMMAND}"
            echo "BENCHMARK: ${BENCHMARK}"
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
                    run_tpch "1"
                    run_tpch_mem "1"
                    run_tpch "10"
                    run_tpch_mem "10"
                    run_cancellation
                    run_parquet
                    run_sort
                    run_clickbench_1
                    run_clickbench_partitioned
                    run_clickbench_extended
                    run_h2o "SMALL" "PARQUET" "groupby"
                    run_h2o "MEDIUM" "PARQUET" "groupby"
                    run_h2o "BIG" "PARQUET" "groupby"
                    run_h2o_join "SMALL" "PARQUET" "join"
                    run_h2o_join "MEDIUM" "PARQUET" "join"
                    run_h2o_join "BIG" "PARQUET" "join"
                    run_imdb
                    run_external_aggr
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
                cancellation)
                    run_cancellation
                    ;;
                parquet)
                    run_parquet
                    ;;
                sort)
                    run_sort
                    ;;
                clickbench_1)
                    run_clickbench_1
                    ;;
                clickbench_partitioned)
                    run_clickbench_partitioned
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
                external_aggr)
                    run_external_aggr
                    ;;
                sort_tpch)
                    run_sort_tpch
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
        docker run -v "${TPCH_DIR}":/data -it --rm ghcr.io/scalytics/tpch-docker:main -vf -s "${SCALE_FACTOR}"
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

    RESULTS_FILE="${RESULTS_DIR}/tpch_sf${SCALE_FACTOR}.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running tpch benchmark..."
    # Optional query filter to run specific query
    QUERY=$([ -n "$ARG3" ] && echo "--query $ARG3" || echo "")
    $CARGO_COMMAND --bin tpch -- benchmark datafusion --iterations 5 --path "${TPCH_DIR}" --prefer_hash_join "${PREFER_HASH_JOIN}" --format parquet -o "${RESULTS_FILE}" $QUERY
}

# Runs the tpch in memory
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
    QUERY=$([ -n "$ARG3" ] && echo "--query $ARG3" || echo "")
    # -m means in memory
    $CARGO_COMMAND --bin tpch -- benchmark datafusion --iterations 5 --path "${TPCH_DIR}" --prefer_hash_join "${PREFER_HASH_JOIN}" -m --format parquet -o "${RESULTS_FILE}" $QUERY
}

# Runs the cancellation benchmark
run_cancellation() {
    RESULTS_FILE="${RESULTS_DIR}/cancellation.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running cancellation benchmark..."
    $CARGO_COMMAND --bin dfbench -- cancellation --iterations 5 --path "${DATA_DIR}/cancellation" -o "${RESULTS_FILE}"
}

# Runs the parquet filter benchmark
run_parquet() {
    RESULTS_FILE="${RESULTS_DIR}/parquet.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running parquet filter benchmark..."
    $CARGO_COMMAND --bin parquet -- filter --path "${DATA_DIR}" --scale-factor 1.0 --iterations 5 -o "${RESULTS_FILE}"
}

# Runs the sort benchmark
run_sort() {
    RESULTS_FILE="${RESULTS_DIR}/sort.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running sort benchmark..."
    $CARGO_COMMAND --bin parquet -- sort --path "${DATA_DIR}" --scale-factor 1.0 --iterations 5 -o "${RESULTS_FILE}"
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
    $CARGO_COMMAND --bin dfbench -- clickbench  --iterations 5 --path "${DATA_DIR}/hits.parquet"  --queries-path "${SCRIPT_DIR}/queries/clickbench/queries.sql" -o "${RESULTS_FILE}"
}

 # Runs the clickbench benchmark with the partitioned parquet files
run_clickbench_partitioned() {
    RESULTS_FILE="${RESULTS_DIR}/clickbench_partitioned.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running clickbench (partitioned, 100 files) benchmark..."
    $CARGO_COMMAND --bin dfbench -- clickbench  --iterations 5 --path "${DATA_DIR}/hits_partitioned" --queries-path "${SCRIPT_DIR}/queries/clickbench/queries.sql" -o "${RESULTS_FILE}"
}

# Runs the clickbench "extended" benchmark with a single large parquet file
run_clickbench_extended() {
    RESULTS_FILE="${RESULTS_DIR}/clickbench_extended.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running clickbench (1 file) extended benchmark..."
    $CARGO_COMMAND --bin dfbench -- clickbench  --iterations 5 --path "${DATA_DIR}/hits.parquet" --queries-path "${SCRIPT_DIR}/queries/clickbench/extended.sql" -o "${RESULTS_FILE}"
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
    $CARGO_COMMAND --bin imdb -- benchmark datafusion --iterations 5 --path "${IMDB_DIR}" --prefer_hash_join "${PREFER_HASH_JOIN}" --format parquet -o "${RESULTS_FILE}"
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
    $CARGO_COMMAND --bin dfbench -- h2o \
        --iterations 3 \
        --path "${H2O_DIR}/${FILE_NAME}" \
        --queries-path "${QUERY_FILE}" \
        -o "${RESULTS_FILE}"
}

run_h2o_join() {
    # Default values for size and data format
    SIZE=${1:-"SMALL"}
    DATA_FORMAT=${2:-"CSV"}
    DATA_FORMAT=$(echo "$DATA_FORMAT" | tr '[:upper:]' '[:lower:]')
    RUN_Type=${3:-"join"}

    # Data directory and results file path
    H2O_DIR="${DATA_DIR}/h2o"
    RESULTS_FILE="${RESULTS_DIR}/h2o_join.json"

    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running h2o join benchmark..."

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

    $CARGO_COMMAND --bin dfbench -- h2o \
        --iterations 3 \
        --join-paths "${H2O_DIR}/${X_TABLE_FILE_NAME},${H2O_DIR}/${SMALL_TABLE_FILE_NAME},${H2O_DIR}/${MEDIUM_TABLE_FILE_NAME},${H2O_DIR}/${LARGE_TABLE_FILE_NAME}" \
        --queries-path "${QUERY_FILE}" \
        -o "${RESULTS_FILE}"
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
    $CARGO_COMMAND --bin external_aggr -- benchmark --partitions 4 --iterations 5 --path "${TPCH_DIR}" -o "${RESULTS_FILE}"
}

# Runs the sort integration benchmark
run_sort_tpch() {
    TPCH_DIR="${DATA_DIR}/tpch_sf1"
    RESULTS_FILE="${RESULTS_DIR}/sort_tpch.json"
    echo "RESULTS_FILE: ${RESULTS_FILE}"
    echo "Running sort tpch benchmark..."

    $CARGO_COMMAND --bin dfbench -- sort-tpch --iterations 5 --path "${TPCH_DIR}" -o "${RESULTS_FILE}"
}


compare_benchmarks() {
    BASE_RESULTS_DIR="${SCRIPT_DIR}/results"
    BRANCH1="$1"
    BRANCH2="$2"
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
            PATH=$VIRTUAL_ENV/bin:$PATH python3 "${SCRIPT_DIR}"/compare.py "${RESULTS_FILE1}" "${RESULTS_FILE2}"
        else
            echo "Note: Skipping ${RESULTS_FILE1} as ${RESULTS_FILE2} does not exist"
        fi
    done

}

setup_venv() {
    python3 -m venv "$VIRTUAL_ENV"
    PATH=$VIRTUAL_ENV/bin:$PATH python3 -m pip install -r requirements.txt
}

# And start the process up
main
